package DBMS;
import java.io.File;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import java.util.*;
import org.junit.Test;
public class MS2_Tests_04
{

	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_35() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("jhej", cols0);
		String tableTrace0 = DBApp.getFullTrace("jhej");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("jhej","a");
		DBApp.createBitMapIndex("jhej","b");
		DBApp.createBitMapIndex("jhej","c");
		DBApp.createBitMapIndex("jhej","d");
		DBApp.createBitMapIndex("jhej","e");
		DBApp.createBitMapIndex("jhej","f");
		DBApp.createBitMapIndex("jhej","g");
		tableTrace0 = DBApp.getFullTrace("jhej");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("jm5i", cols1);
		String tableTrace1 = DBApp.getFullTrace("jm5i");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("jm5i","a");
		DBApp.createBitMapIndex("jm5i","b");
		DBApp.createBitMapIndex("jm5i","c");
		DBApp.createBitMapIndex("jm5i","d");
		DBApp.createBitMapIndex("jm5i","e");
		DBApp.createBitMapIndex("jm5i","f");
		DBApp.createBitMapIndex("jm5i","g");
		tableTrace1 = DBApp.getFullTrace("jm5i");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("kl59m", cols2);
		String tableTrace2 = DBApp.getFullTrace("kl59m");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("kl59m","a");
		DBApp.createBitMapIndex("kl59m","b");
		DBApp.createBitMapIndex("kl59m","c");
		DBApp.createBitMapIndex("kl59m","d");
		DBApp.createBitMapIndex("kl59m","e");
		DBApp.createBitMapIndex("kl59m","f");
		DBApp.createBitMapIndex("kl59m","g");
		tableTrace2 = DBApp.getFullTrace("kl59m");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("ld0t", cols3);
		String tableTrace3 = DBApp.getFullTrace("ld0t");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("ld0t","a");
		DBApp.createBitMapIndex("ld0t","b");
		DBApp.createBitMapIndex("ld0t","c");
		DBApp.createBitMapIndex("ld0t","d");
		DBApp.createBitMapIndex("ld0t","e");
		DBApp.createBitMapIndex("ld0t","f");
		DBApp.createBitMapIndex("ld0t","g");
		DBApp.createBitMapIndex("ld0t","h");
		DBApp.createBitMapIndex("ld0t","i");
		tableTrace3 = DBApp.getFullTrace("ld0t");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g, h, i]"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("dk77r", cols4);
		String tableTrace4 = DBApp.getFullTrace("dk77r");
		assertTrue("Full trace should report indexed columns.", tableTrace4.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace4.endsWith("[]"));
		DBApp.createBitMapIndex("dk77r","a");
		DBApp.createBitMapIndex("dk77r","b");
		DBApp.createBitMapIndex("dk77r","c");
		DBApp.createBitMapIndex("dk77r","d");
		DBApp.createBitMapIndex("dk77r","e");
		DBApp.createBitMapIndex("dk77r","f");
		DBApp.createBitMapIndex("dk77r","g");
		DBApp.createBitMapIndex("dk77r","h");
		DBApp.createBitMapIndex("dk77r","i");
		DBApp.createBitMapIndex("dk77r","j");
		DBApp.createBitMapIndex("dk77r","k");
		tableTrace4 = DBApp.getFullTrace("dk77r");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace4.endsWith("[a, b, c, d, e, f, g, h, i, j, k]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_36() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("pft4h", cols0);
		String tableTrace0 = DBApp.getFullTrace("pft4h");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("pft4h","a");
		DBApp.createBitMapIndex("pft4h","b");
		DBApp.createBitMapIndex("pft4h","c");
		DBApp.createBitMapIndex("pft4h","d");
		DBApp.createBitMapIndex("pft4h","e");
		DBApp.createBitMapIndex("pft4h","f");
		DBApp.createBitMapIndex("pft4h","g");
		DBApp.createBitMapIndex("pft4h","h");
		DBApp.createBitMapIndex("pft4h","i");
		DBApp.createBitMapIndex("pft4h","j");
		DBApp.createBitMapIndex("pft4h","k");
		DBApp.createBitMapIndex("pft4h","l");
		DBApp.createBitMapIndex("pft4h","m");
		DBApp.createBitMapIndex("pft4h","n");
		tableTrace0 = DBApp.getFullTrace("pft4h");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("i9", cols1);
		String tableTrace1 = DBApp.getFullTrace("i9");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("i9","a");
		DBApp.createBitMapIndex("i9","b");
		DBApp.createBitMapIndex("i9","c");
		DBApp.createBitMapIndex("i9","d");
		DBApp.createBitMapIndex("i9","e");
		DBApp.createBitMapIndex("i9","f");
		DBApp.createBitMapIndex("i9","g");
		DBApp.createBitMapIndex("i9","h");
		DBApp.createBitMapIndex("i9","i");
		DBApp.createBitMapIndex("i9","j");
		DBApp.createBitMapIndex("i9","k");
		DBApp.createBitMapIndex("i9","l");
		DBApp.createBitMapIndex("i9","m");
		DBApp.createBitMapIndex("i9","n");
		DBApp.createBitMapIndex("i9","o");
		DBApp.createBitMapIndex("i9","p");
		DBApp.createBitMapIndex("i9","q");
		DBApp.createBitMapIndex("i9","r");
		DBApp.createBitMapIndex("i9","s");
		DBApp.createBitMapIndex("i9","t");
		tableTrace1 = DBApp.getFullTrace("i9");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("x6n9", cols2);
		String tableTrace2 = DBApp.getFullTrace("x6n9");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("x6n9","a");
		DBApp.createBitMapIndex("x6n9","b");
		DBApp.createBitMapIndex("x6n9","c");
		DBApp.createBitMapIndex("x6n9","d");
		DBApp.createBitMapIndex("x6n9","e");
		DBApp.createBitMapIndex("x6n9","f");
		DBApp.createBitMapIndex("x6n9","g");
		DBApp.createBitMapIndex("x6n9","h");
		DBApp.createBitMapIndex("x6n9","i");
		DBApp.createBitMapIndex("x6n9","j");
		DBApp.createBitMapIndex("x6n9","k");
		DBApp.createBitMapIndex("x6n9","l");
		DBApp.createBitMapIndex("x6n9","m");
		DBApp.createBitMapIndex("x6n9","n");
		DBApp.createBitMapIndex("x6n9","o");
		DBApp.createBitMapIndex("x6n9","p");
		DBApp.createBitMapIndex("x6n9","q");
		tableTrace2 = DBApp.getFullTrace("x6n9");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("zp1", cols3);
		String tableTrace3 = DBApp.getFullTrace("zp1");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("zp1","a");
		DBApp.createBitMapIndex("zp1","b");
		DBApp.createBitMapIndex("zp1","c");
		DBApp.createBitMapIndex("zp1","d");
		DBApp.createBitMapIndex("zp1","e");
		DBApp.createBitMapIndex("zp1","f");
		DBApp.createBitMapIndex("zp1","g");
		tableTrace3 = DBApp.getFullTrace("zp1");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("cit", cols4);
		String tableTrace4 = DBApp.getFullTrace("cit");
		assertTrue("Full trace should report indexed columns.", tableTrace4.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace4.endsWith("[]"));
		DBApp.createBitMapIndex("cit","a");
		DBApp.createBitMapIndex("cit","b");
		DBApp.createBitMapIndex("cit","c");
		DBApp.createBitMapIndex("cit","d");
		DBApp.createBitMapIndex("cit","e");
		DBApp.createBitMapIndex("cit","f");
		DBApp.createBitMapIndex("cit","g");
		DBApp.createBitMapIndex("cit","h");
		DBApp.createBitMapIndex("cit","i");
		DBApp.createBitMapIndex("cit","j");
		DBApp.createBitMapIndex("cit","k");
		tableTrace4 = DBApp.getFullTrace("cit");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace4.endsWith("[a, b, c, d, e, f, g, h, i, j, k]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_37() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("li5d", cols0);
		String tableTrace0 = DBApp.getFullTrace("li5d");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("li5d","a");
		DBApp.createBitMapIndex("li5d","b");
		DBApp.createBitMapIndex("li5d","c");
		DBApp.createBitMapIndex("li5d","d");
		DBApp.createBitMapIndex("li5d","e");
		DBApp.createBitMapIndex("li5d","f");
		DBApp.createBitMapIndex("li5d","g");
		DBApp.createBitMapIndex("li5d","h");
		DBApp.createBitMapIndex("li5d","i");
		DBApp.createBitMapIndex("li5d","j");
		DBApp.createBitMapIndex("li5d","k");
		DBApp.createBitMapIndex("li5d","l");
		DBApp.createBitMapIndex("li5d","m");
		DBApp.createBitMapIndex("li5d","n");
		DBApp.createBitMapIndex("li5d","o");
		tableTrace0 = DBApp.getFullTrace("li5d");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_38() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("wx75", cols0);
		String tableTrace0 = DBApp.getFullTrace("wx75");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("wx75","a");
		DBApp.createBitMapIndex("wx75","b");
		DBApp.createBitMapIndex("wx75","c");
		DBApp.createBitMapIndex("wx75","d");
		DBApp.createBitMapIndex("wx75","e");
		DBApp.createBitMapIndex("wx75","f");
		DBApp.createBitMapIndex("wx75","g");
		DBApp.createBitMapIndex("wx75","h");
		DBApp.createBitMapIndex("wx75","i");
		DBApp.createBitMapIndex("wx75","j");
		DBApp.createBitMapIndex("wx75","k");
		DBApp.createBitMapIndex("wx75","l");
		DBApp.createBitMapIndex("wx75","m");
		DBApp.createBitMapIndex("wx75","n");
		DBApp.createBitMapIndex("wx75","o");
		DBApp.createBitMapIndex("wx75","p");
		DBApp.createBitMapIndex("wx75","q");
		DBApp.createBitMapIndex("wx75","r");
		DBApp.createBitMapIndex("wx75","s");
		tableTrace0 = DBApp.getFullTrace("wx75");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_39() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("lbi", cols0);
		String tableTrace0 = DBApp.getFullTrace("lbi");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("lbi","a");
		DBApp.createBitMapIndex("lbi","b");
		DBApp.createBitMapIndex("lbi","c");
		DBApp.createBitMapIndex("lbi","d");
		DBApp.createBitMapIndex("lbi","e");
		DBApp.createBitMapIndex("lbi","f");
		DBApp.createBitMapIndex("lbi","g");
		tableTrace0 = DBApp.getFullTrace("lbi");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("o78", cols1);
		String tableTrace1 = DBApp.getFullTrace("o78");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("o78","a");
		DBApp.createBitMapIndex("o78","b");
		DBApp.createBitMapIndex("o78","c");
		DBApp.createBitMapIndex("o78","d");
		DBApp.createBitMapIndex("o78","e");
		DBApp.createBitMapIndex("o78","f");
		DBApp.createBitMapIndex("o78","g");
		DBApp.createBitMapIndex("o78","h");
		DBApp.createBitMapIndex("o78","i");
		DBApp.createBitMapIndex("o78","j");
		DBApp.createBitMapIndex("o78","k");
		DBApp.createBitMapIndex("o78","l");
		DBApp.createBitMapIndex("o78","m");
		DBApp.createBitMapIndex("o78","n");
		DBApp.createBitMapIndex("o78","o");
		DBApp.createBitMapIndex("o78","p");
		tableTrace1 = DBApp.getFullTrace("o78");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("pi9r", cols2);
		String tableTrace2 = DBApp.getFullTrace("pi9r");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("pi9r","a");
		DBApp.createBitMapIndex("pi9r","b");
		DBApp.createBitMapIndex("pi9r","c");
		DBApp.createBitMapIndex("pi9r","d");
		DBApp.createBitMapIndex("pi9r","e");
		DBApp.createBitMapIndex("pi9r","f");
		DBApp.createBitMapIndex("pi9r","g");
		DBApp.createBitMapIndex("pi9r","h");
		DBApp.createBitMapIndex("pi9r","i");
		DBApp.createBitMapIndex("pi9r","j");
		tableTrace2 = DBApp.getFullTrace("pi9r");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("y1", cols3);
		String tableTrace3 = DBApp.getFullTrace("y1");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("y1","a");
		DBApp.createBitMapIndex("y1","b");
		DBApp.createBitMapIndex("y1","c");
		DBApp.createBitMapIndex("y1","d");
		DBApp.createBitMapIndex("y1","e");
		DBApp.createBitMapIndex("y1","f");
		DBApp.createBitMapIndex("y1","g");
		DBApp.createBitMapIndex("y1","h");
		DBApp.createBitMapIndex("y1","i");
		DBApp.createBitMapIndex("y1","j");
		DBApp.createBitMapIndex("y1","k");
		DBApp.createBitMapIndex("y1","l");
		DBApp.createBitMapIndex("y1","m");
		DBApp.createBitMapIndex("y1","n");
		DBApp.createBitMapIndex("y1","o");
		DBApp.createBitMapIndex("y1","p");
		DBApp.createBitMapIndex("y1","q");
		tableTrace3 = DBApp.getFullTrace("y1");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q]"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("k98g", cols4);
		String tableTrace4 = DBApp.getFullTrace("k98g");
		assertTrue("Full trace should report indexed columns.", tableTrace4.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace4.endsWith("[]"));
		DBApp.createBitMapIndex("k98g","a");
		DBApp.createBitMapIndex("k98g","b");
		DBApp.createBitMapIndex("k98g","c");
		DBApp.createBitMapIndex("k98g","d");
		DBApp.createBitMapIndex("k98g","e");
		DBApp.createBitMapIndex("k98g","f");
		DBApp.createBitMapIndex("k98g","g");
		DBApp.createBitMapIndex("k98g","h");
		DBApp.createBitMapIndex("k98g","i");
		DBApp.createBitMapIndex("k98g","j");
		DBApp.createBitMapIndex("k98g","k");
		DBApp.createBitMapIndex("k98g","l");
		DBApp.createBitMapIndex("k98g","m");
		DBApp.createBitMapIndex("k98g","n");
		DBApp.createBitMapIndex("k98g","o");
		DBApp.createBitMapIndex("k98g","p");
		DBApp.createBitMapIndex("k98g","q");
		tableTrace4 = DBApp.getFullTrace("k98g");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace4.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_40() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("z714c", cols0);
		String tableTrace0 = DBApp.getFullTrace("z714c");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("z714c","a");
		DBApp.createBitMapIndex("z714c","b");
		DBApp.createBitMapIndex("z714c","c");
		DBApp.createBitMapIndex("z714c","d");
		DBApp.createBitMapIndex("z714c","e");
		DBApp.createBitMapIndex("z714c","f");
		DBApp.createBitMapIndex("z714c","g");
		tableTrace0 = DBApp.getFullTrace("z714c");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("b1h", cols1);
		String tableTrace1 = DBApp.getFullTrace("b1h");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("b1h","a");
		DBApp.createBitMapIndex("b1h","b");
		DBApp.createBitMapIndex("b1h","c");
		DBApp.createBitMapIndex("b1h","d");
		DBApp.createBitMapIndex("b1h","e");
		DBApp.createBitMapIndex("b1h","f");
		DBApp.createBitMapIndex("b1h","g");
		DBApp.createBitMapIndex("b1h","h");
		DBApp.createBitMapIndex("b1h","i");
		DBApp.createBitMapIndex("b1h","j");
		DBApp.createBitMapIndex("b1h","k");
		DBApp.createBitMapIndex("b1h","l");
		DBApp.createBitMapIndex("b1h","m");
		DBApp.createBitMapIndex("b1h","n");
		DBApp.createBitMapIndex("b1h","o");
		tableTrace1 = DBApp.getFullTrace("b1h");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("l7", cols2);
		String tableTrace2 = DBApp.getFullTrace("l7");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("l7","a");
		DBApp.createBitMapIndex("l7","b");
		DBApp.createBitMapIndex("l7","c");
		DBApp.createBitMapIndex("l7","d");
		DBApp.createBitMapIndex("l7","e");
		DBApp.createBitMapIndex("l7","f");
		DBApp.createBitMapIndex("l7","g");
		DBApp.createBitMapIndex("l7","h");
		DBApp.createBitMapIndex("l7","i");
		DBApp.createBitMapIndex("l7","j");
		DBApp.createBitMapIndex("l7","k");
		DBApp.createBitMapIndex("l7","l");
		DBApp.createBitMapIndex("l7","m");
		DBApp.createBitMapIndex("l7","n");
		DBApp.createBitMapIndex("l7","o");
		tableTrace2 = DBApp.getFullTrace("l7");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_41() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("f1u", cols0);
		String tableTrace0 = DBApp.getFullTrace("f1u");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("f1u","a");
		DBApp.createBitMapIndex("f1u","b");
		DBApp.createBitMapIndex("f1u","c");
		DBApp.createBitMapIndex("f1u","d");
		DBApp.createBitMapIndex("f1u","e");
		DBApp.createBitMapIndex("f1u","f");
		DBApp.createBitMapIndex("f1u","g");
		DBApp.createBitMapIndex("f1u","h");
		DBApp.createBitMapIndex("f1u","i");
		DBApp.createBitMapIndex("f1u","j");
		DBApp.createBitMapIndex("f1u","k");
		DBApp.createBitMapIndex("f1u","l");
		DBApp.createBitMapIndex("f1u","m");
		DBApp.createBitMapIndex("f1u","n");
		tableTrace0 = DBApp.getFullTrace("f1u");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("uez", cols1);
		String tableTrace1 = DBApp.getFullTrace("uez");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("uez","a");
		DBApp.createBitMapIndex("uez","b");
		DBApp.createBitMapIndex("uez","c");
		DBApp.createBitMapIndex("uez","d");
		DBApp.createBitMapIndex("uez","e");
		DBApp.createBitMapIndex("uez","f");
		DBApp.createBitMapIndex("uez","g");
		tableTrace1 = DBApp.getFullTrace("uez");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_42() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("xi3", cols0);
		String tableTrace0 = DBApp.getFullTrace("xi3");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("xi3","a");
		DBApp.createBitMapIndex("xi3","b");
		DBApp.createBitMapIndex("xi3","c");
		DBApp.createBitMapIndex("xi3","d");
		DBApp.createBitMapIndex("xi3","e");
		DBApp.createBitMapIndex("xi3","f");
		DBApp.createBitMapIndex("xi3","g");
		tableTrace0 = DBApp.getFullTrace("xi3");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_43() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 10;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("p24", cols0);
		String tableTrace0 = DBApp.getFullTrace("p24");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("p24","a");
		DBApp.createBitMapIndex("p24","b");
		DBApp.createBitMapIndex("p24","c");
		DBApp.createBitMapIndex("p24","d");
		DBApp.createBitMapIndex("p24","e");
		DBApp.createBitMapIndex("p24","f");
		DBApp.createBitMapIndex("p24","g");
		tableTrace0 = DBApp.getFullTrace("p24");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("u75", cols1);
		String tableTrace1 = DBApp.getFullTrace("u75");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("u75","a");
		DBApp.createBitMapIndex("u75","b");
		DBApp.createBitMapIndex("u75","c");
		DBApp.createBitMapIndex("u75","d");
		DBApp.createBitMapIndex("u75","e");
		DBApp.createBitMapIndex("u75","f");
		DBApp.createBitMapIndex("u75","g");
		DBApp.createBitMapIndex("u75","h");
		DBApp.createBitMapIndex("u75","i");
		DBApp.createBitMapIndex("u75","j");
		DBApp.createBitMapIndex("u75","k");
		DBApp.createBitMapIndex("u75","l");
		DBApp.createBitMapIndex("u75","m");
		DBApp.createBitMapIndex("u75","n");
		DBApp.createBitMapIndex("u75","o");
		tableTrace1 = DBApp.getFullTrace("u75");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("y1", cols2);
		String tableTrace2 = DBApp.getFullTrace("y1");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("y1","a");
		DBApp.createBitMapIndex("y1","b");
		DBApp.createBitMapIndex("y1","c");
		DBApp.createBitMapIndex("y1","d");
		DBApp.createBitMapIndex("y1","e");
		DBApp.createBitMapIndex("y1","f");
		DBApp.createBitMapIndex("y1","g");
		DBApp.createBitMapIndex("y1","h");
		tableTrace2 = DBApp.getFullTrace("y1");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("ie5", cols3);
		String tableTrace3 = DBApp.getFullTrace("ie5");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("ie5","a");
		DBApp.createBitMapIndex("ie5","b");
		DBApp.createBitMapIndex("ie5","c");
		DBApp.createBitMapIndex("ie5","d");
		DBApp.createBitMapIndex("ie5","e");
		DBApp.createBitMapIndex("ie5","f");
		DBApp.createBitMapIndex("ie5","g");
		DBApp.createBitMapIndex("ie5","h");
		DBApp.createBitMapIndex("ie5","i");
		DBApp.createBitMapIndex("ie5","j");
		DBApp.createBitMapIndex("ie5","k");
		tableTrace3 = DBApp.getFullTrace("ie5");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g, h, i, j, k]"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("jm", cols4);
		String tableTrace4 = DBApp.getFullTrace("jm");
		assertTrue("Full trace should report indexed columns.", tableTrace4.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace4.endsWith("[]"));
		DBApp.createBitMapIndex("jm","a");
		DBApp.createBitMapIndex("jm","b");
		DBApp.createBitMapIndex("jm","c");
		DBApp.createBitMapIndex("jm","d");
		DBApp.createBitMapIndex("jm","e");
		DBApp.createBitMapIndex("jm","f");
		DBApp.createBitMapIndex("jm","g");
		DBApp.createBitMapIndex("jm","h");
		DBApp.createBitMapIndex("jm","i");
		tableTrace4 = DBApp.getFullTrace("jm");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace4.endsWith("[a, b, c, d, e, f, g, h, i]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_44() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("c5w04", cols0);
		String tableTrace0 = DBApp.getFullTrace("c5w04");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("c5w04","a");
		DBApp.createBitMapIndex("c5w04","b");
		DBApp.createBitMapIndex("c5w04","c");
		DBApp.createBitMapIndex("c5w04","d");
		DBApp.createBitMapIndex("c5w04","e");
		DBApp.createBitMapIndex("c5w04","f");
		DBApp.createBitMapIndex("c5w04","g");
		tableTrace0 = DBApp.getFullTrace("c5w04");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("z0", cols1);
		String tableTrace1 = DBApp.getFullTrace("z0");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("z0","a");
		DBApp.createBitMapIndex("z0","b");
		DBApp.createBitMapIndex("z0","c");
		DBApp.createBitMapIndex("z0","d");
		DBApp.createBitMapIndex("z0","e");
		DBApp.createBitMapIndex("z0","f");
		DBApp.createBitMapIndex("z0","g");
		DBApp.createBitMapIndex("z0","h");
		DBApp.createBitMapIndex("z0","i");
		DBApp.createBitMapIndex("z0","j");
		DBApp.createBitMapIndex("z0","k");
		DBApp.createBitMapIndex("z0","l");
		DBApp.createBitMapIndex("z0","m");
		DBApp.createBitMapIndex("z0","n");
		DBApp.createBitMapIndex("z0","o");
		DBApp.createBitMapIndex("z0","p");
		DBApp.createBitMapIndex("z0","q");
		DBApp.createBitMapIndex("z0","r");
		DBApp.createBitMapIndex("z0","s");
		tableTrace1 = DBApp.getFullTrace("z0");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_45() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("eb4g", cols0);
		String tableTrace0 = DBApp.getFullTrace("eb4g");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("eb4g","a");
		DBApp.createBitMapIndex("eb4g","b");
		DBApp.createBitMapIndex("eb4g","c");
		DBApp.createBitMapIndex("eb4g","d");
		DBApp.createBitMapIndex("eb4g","e");
		DBApp.createBitMapIndex("eb4g","f");
		DBApp.createBitMapIndex("eb4g","g");
		tableTrace0 = DBApp.getFullTrace("eb4g");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("i7gcj", cols1);
		String tableTrace1 = DBApp.getFullTrace("i7gcj");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("i7gcj","a");
		DBApp.createBitMapIndex("i7gcj","b");
		DBApp.createBitMapIndex("i7gcj","c");
		DBApp.createBitMapIndex("i7gcj","d");
		DBApp.createBitMapIndex("i7gcj","e");
		DBApp.createBitMapIndex("i7gcj","f");
		DBApp.createBitMapIndex("i7gcj","g");
		tableTrace1 = DBApp.getFullTrace("i7gcj");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_46() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("o4", cols0);
		String tableTrace0 = DBApp.getFullTrace("o4");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("o4","a");
		DBApp.createBitMapIndex("o4","b");
		DBApp.createBitMapIndex("o4","c");
		DBApp.createBitMapIndex("o4","d");
		DBApp.createBitMapIndex("o4","e");
		DBApp.createBitMapIndex("o4","f");
		DBApp.createBitMapIndex("o4","g");
		DBApp.createBitMapIndex("o4","h");
		DBApp.createBitMapIndex("o4","i");
		DBApp.createBitMapIndex("o4","j");
		DBApp.createBitMapIndex("o4","k");
		DBApp.createBitMapIndex("o4","l");
		DBApp.createBitMapIndex("o4","m");
		DBApp.createBitMapIndex("o4","n");
		DBApp.createBitMapIndex("o4","o");
		DBApp.createBitMapIndex("o4","p");
		DBApp.createBitMapIndex("o4","q");
		DBApp.createBitMapIndex("o4","r");
		tableTrace0 = DBApp.getFullTrace("o4");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("q70f", cols1);
		String tableTrace1 = DBApp.getFullTrace("q70f");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("q70f","a");
		DBApp.createBitMapIndex("q70f","b");
		DBApp.createBitMapIndex("q70f","c");
		DBApp.createBitMapIndex("q70f","d");
		DBApp.createBitMapIndex("q70f","e");
		DBApp.createBitMapIndex("q70f","f");
		DBApp.createBitMapIndex("q70f","g");
		DBApp.createBitMapIndex("q70f","h");
		DBApp.createBitMapIndex("q70f","i");
		DBApp.createBitMapIndex("q70f","j");
		DBApp.createBitMapIndex("q70f","k");
		DBApp.createBitMapIndex("q70f","l");
		tableTrace1 = DBApp.getFullTrace("q70f");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("cvy0", cols2);
		String tableTrace2 = DBApp.getFullTrace("cvy0");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("cvy0","a");
		DBApp.createBitMapIndex("cvy0","b");
		DBApp.createBitMapIndex("cvy0","c");
		DBApp.createBitMapIndex("cvy0","d");
		DBApp.createBitMapIndex("cvy0","e");
		DBApp.createBitMapIndex("cvy0","f");
		DBApp.createBitMapIndex("cvy0","g");
		DBApp.createBitMapIndex("cvy0","h");
		DBApp.createBitMapIndex("cvy0","i");
		DBApp.createBitMapIndex("cvy0","j");
		DBApp.createBitMapIndex("cvy0","k");
		DBApp.createBitMapIndex("cvy0","l");
		DBApp.createBitMapIndex("cvy0","m");
		DBApp.createBitMapIndex("cvy0","n");
		DBApp.createBitMapIndex("cvy0","o");
		DBApp.createBitMapIndex("cvy0","p");
		DBApp.createBitMapIndex("cvy0","q");
		DBApp.createBitMapIndex("cvy0","r");
		DBApp.createBitMapIndex("cvy0","s");
		DBApp.createBitMapIndex("cvy0","t");
		tableTrace2 = DBApp.getFullTrace("cvy0");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("e0p", cols3);
		String tableTrace3 = DBApp.getFullTrace("e0p");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("e0p","a");
		DBApp.createBitMapIndex("e0p","b");
		DBApp.createBitMapIndex("e0p","c");
		DBApp.createBitMapIndex("e0p","d");
		DBApp.createBitMapIndex("e0p","e");
		DBApp.createBitMapIndex("e0p","f");
		DBApp.createBitMapIndex("e0p","g");
		tableTrace3 = DBApp.getFullTrace("e0p");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_47() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("u9e", cols0);
		String tableTrace0 = DBApp.getFullTrace("u9e");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("u9e","a");
		DBApp.createBitMapIndex("u9e","b");
		DBApp.createBitMapIndex("u9e","c");
		DBApp.createBitMapIndex("u9e","d");
		DBApp.createBitMapIndex("u9e","e");
		DBApp.createBitMapIndex("u9e","f");
		DBApp.createBitMapIndex("u9e","g");
		DBApp.createBitMapIndex("u9e","h");
		DBApp.createBitMapIndex("u9e","i");
		DBApp.createBitMapIndex("u9e","j");
		DBApp.createBitMapIndex("u9e","k");
		DBApp.createBitMapIndex("u9e","l");
		tableTrace0 = DBApp.getFullTrace("u9e");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("idw", cols1);
		String tableTrace1 = DBApp.getFullTrace("idw");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("idw","a");
		DBApp.createBitMapIndex("idw","b");
		DBApp.createBitMapIndex("idw","c");
		DBApp.createBitMapIndex("idw","d");
		DBApp.createBitMapIndex("idw","e");
		DBApp.createBitMapIndex("idw","f");
		DBApp.createBitMapIndex("idw","g");
		DBApp.createBitMapIndex("idw","h");
		DBApp.createBitMapIndex("idw","i");
		DBApp.createBitMapIndex("idw","j");
		DBApp.createBitMapIndex("idw","k");
		DBApp.createBitMapIndex("idw","l");
		DBApp.createBitMapIndex("idw","m");
		DBApp.createBitMapIndex("idw","n");
		DBApp.createBitMapIndex("idw","o");
		tableTrace1 = DBApp.getFullTrace("idw");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_48() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("ar94", cols0);
		String tableTrace0 = DBApp.getFullTrace("ar94");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("ar94","a");
		DBApp.createBitMapIndex("ar94","b");
		DBApp.createBitMapIndex("ar94","c");
		DBApp.createBitMapIndex("ar94","d");
		DBApp.createBitMapIndex("ar94","e");
		DBApp.createBitMapIndex("ar94","f");
		DBApp.createBitMapIndex("ar94","g");
		DBApp.createBitMapIndex("ar94","h");
		DBApp.createBitMapIndex("ar94","i");
		DBApp.createBitMapIndex("ar94","j");
		DBApp.createBitMapIndex("ar94","k");
		DBApp.createBitMapIndex("ar94","l");
		DBApp.createBitMapIndex("ar94","m");
		DBApp.createBitMapIndex("ar94","n");
		DBApp.createBitMapIndex("ar94","o");
		DBApp.createBitMapIndex("ar94","p");
		DBApp.createBitMapIndex("ar94","q");
		DBApp.createBitMapIndex("ar94","r");
		tableTrace0 = DBApp.getFullTrace("ar94");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("ijk", cols1);
		String tableTrace1 = DBApp.getFullTrace("ijk");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("ijk","a");
		DBApp.createBitMapIndex("ijk","b");
		DBApp.createBitMapIndex("ijk","c");
		DBApp.createBitMapIndex("ijk","d");
		DBApp.createBitMapIndex("ijk","e");
		DBApp.createBitMapIndex("ijk","f");
		DBApp.createBitMapIndex("ijk","g");
		DBApp.createBitMapIndex("ijk","h");
		DBApp.createBitMapIndex("ijk","i");
		DBApp.createBitMapIndex("ijk","j");
		DBApp.createBitMapIndex("ijk","k");
		DBApp.createBitMapIndex("ijk","l");
		DBApp.createBitMapIndex("ijk","m");
		DBApp.createBitMapIndex("ijk","n");
		DBApp.createBitMapIndex("ijk","o");
		DBApp.createBitMapIndex("ijk","p");
		DBApp.createBitMapIndex("ijk","q");
		DBApp.createBitMapIndex("ijk","r");
		tableTrace1 = DBApp.getFullTrace("ijk");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("f9", cols2);
		String tableTrace2 = DBApp.getFullTrace("f9");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("f9","a");
		DBApp.createBitMapIndex("f9","b");
		DBApp.createBitMapIndex("f9","c");
		DBApp.createBitMapIndex("f9","d");
		DBApp.createBitMapIndex("f9","e");
		DBApp.createBitMapIndex("f9","f");
		DBApp.createBitMapIndex("f9","g");
		DBApp.createBitMapIndex("f9","h");
		DBApp.createBitMapIndex("f9","i");
		DBApp.createBitMapIndex("f9","j");
		DBApp.createBitMapIndex("f9","k");
		DBApp.createBitMapIndex("f9","l");
		DBApp.createBitMapIndex("f9","m");
		DBApp.createBitMapIndex("f9","n");
		DBApp.createBitMapIndex("f9","o");
		DBApp.createBitMapIndex("f9","p");
		DBApp.createBitMapIndex("f9","q");
		DBApp.createBitMapIndex("f9","r");
		DBApp.createBitMapIndex("f9","s");
		tableTrace2 = DBApp.getFullTrace("f9");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("bo6", cols3);
		String tableTrace3 = DBApp.getFullTrace("bo6");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("bo6","a");
		DBApp.createBitMapIndex("bo6","b");
		DBApp.createBitMapIndex("bo6","c");
		DBApp.createBitMapIndex("bo6","d");
		DBApp.createBitMapIndex("bo6","e");
		DBApp.createBitMapIndex("bo6","f");
		DBApp.createBitMapIndex("bo6","g");
		DBApp.createBitMapIndex("bo6","h");
		DBApp.createBitMapIndex("bo6","i");
		DBApp.createBitMapIndex("bo6","j");
		DBApp.createBitMapIndex("bo6","k");
		DBApp.createBitMapIndex("bo6","l");
		DBApp.createBitMapIndex("bo6","m");
		tableTrace3 = DBApp.getFullTrace("bo6");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_49() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("fen6", cols0);
		String tableTrace0 = DBApp.getFullTrace("fen6");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("fen6","a");
		DBApp.createBitMapIndex("fen6","b");
		DBApp.createBitMapIndex("fen6","c");
		DBApp.createBitMapIndex("fen6","d");
		DBApp.createBitMapIndex("fen6","e");
		DBApp.createBitMapIndex("fen6","f");
		DBApp.createBitMapIndex("fen6","g");
		DBApp.createBitMapIndex("fen6","h");
		DBApp.createBitMapIndex("fen6","i");
		DBApp.createBitMapIndex("fen6","j");
		DBApp.createBitMapIndex("fen6","k");
		DBApp.createBitMapIndex("fen6","l");
		DBApp.createBitMapIndex("fen6","m");
		DBApp.createBitMapIndex("fen6","n");
		DBApp.createBitMapIndex("fen6","o");
		DBApp.createBitMapIndex("fen6","p");
		DBApp.createBitMapIndex("fen6","q");
		DBApp.createBitMapIndex("fen6","r");
		DBApp.createBitMapIndex("fen6","s");
		DBApp.createBitMapIndex("fen6","t");
		tableTrace0 = DBApp.getFullTrace("fen6");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("q9", cols1);
		String tableTrace1 = DBApp.getFullTrace("q9");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("q9","a");
		DBApp.createBitMapIndex("q9","b");
		DBApp.createBitMapIndex("q9","c");
		DBApp.createBitMapIndex("q9","d");
		DBApp.createBitMapIndex("q9","e");
		DBApp.createBitMapIndex("q9","f");
		DBApp.createBitMapIndex("q9","g");
		DBApp.createBitMapIndex("q9","h");
		DBApp.createBitMapIndex("q9","i");
		DBApp.createBitMapIndex("q9","j");
		DBApp.createBitMapIndex("q9","k");
		DBApp.createBitMapIndex("q9","l");
		DBApp.createBitMapIndex("q9","m");
		DBApp.createBitMapIndex("q9","n");
		DBApp.createBitMapIndex("q9","o");
		DBApp.createBitMapIndex("q9","p");
		DBApp.createBitMapIndex("q9","q");
		DBApp.createBitMapIndex("q9","r");
		DBApp.createBitMapIndex("q9","s");
		DBApp.createBitMapIndex("q9","t");
		tableTrace1 = DBApp.getFullTrace("q9");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("gl", cols2);
		String tableTrace2 = DBApp.getFullTrace("gl");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("gl","a");
		DBApp.createBitMapIndex("gl","b");
		DBApp.createBitMapIndex("gl","c");
		DBApp.createBitMapIndex("gl","d");
		DBApp.createBitMapIndex("gl","e");
		DBApp.createBitMapIndex("gl","f");
		DBApp.createBitMapIndex("gl","g");
		DBApp.createBitMapIndex("gl","h");
		DBApp.createBitMapIndex("gl","i");
		DBApp.createBitMapIndex("gl","j");
		DBApp.createBitMapIndex("gl","k");
		DBApp.createBitMapIndex("gl","l");
		DBApp.createBitMapIndex("gl","m");
		DBApp.createBitMapIndex("gl","n");
		DBApp.createBitMapIndex("gl","o");
		tableTrace2 = DBApp.getFullTrace("gl");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_50() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("zq", cols0);
		String tableTrace0 = DBApp.getFullTrace("zq");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("zq","a");
		DBApp.createBitMapIndex("zq","b");
		DBApp.createBitMapIndex("zq","c");
		DBApp.createBitMapIndex("zq","d");
		DBApp.createBitMapIndex("zq","e");
		DBApp.createBitMapIndex("zq","f");
		DBApp.createBitMapIndex("zq","g");
		tableTrace0 = DBApp.getFullTrace("zq");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("w02d", cols1);
		String tableTrace1 = DBApp.getFullTrace("w02d");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("w02d","a");
		DBApp.createBitMapIndex("w02d","b");
		DBApp.createBitMapIndex("w02d","c");
		DBApp.createBitMapIndex("w02d","d");
		DBApp.createBitMapIndex("w02d","e");
		DBApp.createBitMapIndex("w02d","f");
		DBApp.createBitMapIndex("w02d","g");
		DBApp.createBitMapIndex("w02d","h");
		DBApp.createBitMapIndex("w02d","i");
		DBApp.createBitMapIndex("w02d","j");
		DBApp.createBitMapIndex("w02d","k");
		DBApp.createBitMapIndex("w02d","l");
		DBApp.createBitMapIndex("w02d","m");
		DBApp.createBitMapIndex("w02d","n");
		DBApp.createBitMapIndex("w02d","o");
		tableTrace1 = DBApp.getFullTrace("w02d");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_51() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("z628z", cols0);
		String tableTrace0 = DBApp.getFullTrace("z628z");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("z628z","a");
		DBApp.createBitMapIndex("z628z","b");
		DBApp.createBitMapIndex("z628z","c");
		DBApp.createBitMapIndex("z628z","d");
		DBApp.createBitMapIndex("z628z","e");
		DBApp.createBitMapIndex("z628z","f");
		DBApp.createBitMapIndex("z628z","g");
		tableTrace0 = DBApp.getFullTrace("z628z");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("p1cvs", cols1);
		String tableTrace1 = DBApp.getFullTrace("p1cvs");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("p1cvs","a");
		DBApp.createBitMapIndex("p1cvs","b");
		DBApp.createBitMapIndex("p1cvs","c");
		DBApp.createBitMapIndex("p1cvs","d");
		DBApp.createBitMapIndex("p1cvs","e");
		DBApp.createBitMapIndex("p1cvs","f");
		DBApp.createBitMapIndex("p1cvs","g");
		DBApp.createBitMapIndex("p1cvs","h");
		DBApp.createBitMapIndex("p1cvs","i");
		DBApp.createBitMapIndex("p1cvs","j");
		DBApp.createBitMapIndex("p1cvs","k");
		DBApp.createBitMapIndex("p1cvs","l");
		DBApp.createBitMapIndex("p1cvs","m");
		tableTrace1 = DBApp.getFullTrace("p1cvs");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("twazr", cols2);
		String tableTrace2 = DBApp.getFullTrace("twazr");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("twazr","a");
		DBApp.createBitMapIndex("twazr","b");
		DBApp.createBitMapIndex("twazr","c");
		DBApp.createBitMapIndex("twazr","d");
		DBApp.createBitMapIndex("twazr","e");
		DBApp.createBitMapIndex("twazr","f");
		DBApp.createBitMapIndex("twazr","g");
		DBApp.createBitMapIndex("twazr","h");
		DBApp.createBitMapIndex("twazr","i");
		DBApp.createBitMapIndex("twazr","j");
		DBApp.createBitMapIndex("twazr","k");
		DBApp.createBitMapIndex("twazr","l");
		DBApp.createBitMapIndex("twazr","m");
		DBApp.createBitMapIndex("twazr","n");
		tableTrace2 = DBApp.getFullTrace("twazr");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_52() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("gu3p4", cols0);
		String tableTrace0 = DBApp.getFullTrace("gu3p4");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("gu3p4","a");
		DBApp.createBitMapIndex("gu3p4","b");
		DBApp.createBitMapIndex("gu3p4","c");
		DBApp.createBitMapIndex("gu3p4","d");
		DBApp.createBitMapIndex("gu3p4","e");
		DBApp.createBitMapIndex("gu3p4","f");
		DBApp.createBitMapIndex("gu3p4","g");
		DBApp.createBitMapIndex("gu3p4","h");
		DBApp.createBitMapIndex("gu3p4","i");
		DBApp.createBitMapIndex("gu3p4","j");
		DBApp.createBitMapIndex("gu3p4","k");
		DBApp.createBitMapIndex("gu3p4","l");
		DBApp.createBitMapIndex("gu3p4","m");
		DBApp.createBitMapIndex("gu3p4","n");
		DBApp.createBitMapIndex("gu3p4","o");
		DBApp.createBitMapIndex("gu3p4","p");
		DBApp.createBitMapIndex("gu3p4","q");
		DBApp.createBitMapIndex("gu3p4","r");
		tableTrace0 = DBApp.getFullTrace("gu3p4");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("g1o", cols1);
		String tableTrace1 = DBApp.getFullTrace("g1o");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("g1o","a");
		DBApp.createBitMapIndex("g1o","b");
		DBApp.createBitMapIndex("g1o","c");
		DBApp.createBitMapIndex("g1o","d");
		DBApp.createBitMapIndex("g1o","e");
		DBApp.createBitMapIndex("g1o","f");
		DBApp.createBitMapIndex("g1o","g");
		DBApp.createBitMapIndex("g1o","h");
		DBApp.createBitMapIndex("g1o","i");
		DBApp.createBitMapIndex("g1o","j");
		DBApp.createBitMapIndex("g1o","k");
		DBApp.createBitMapIndex("g1o","l");
		DBApp.createBitMapIndex("g1o","m");
		DBApp.createBitMapIndex("g1o","n");
		DBApp.createBitMapIndex("g1o","o");
		DBApp.createBitMapIndex("g1o","p");
		DBApp.createBitMapIndex("g1o","q");
		DBApp.createBitMapIndex("g1o","r");
		DBApp.createBitMapIndex("g1o","s");
		tableTrace1 = DBApp.getFullTrace("g1o");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("a55", cols2);
		String tableTrace2 = DBApp.getFullTrace("a55");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("a55","a");
		DBApp.createBitMapIndex("a55","b");
		DBApp.createBitMapIndex("a55","c");
		DBApp.createBitMapIndex("a55","d");
		DBApp.createBitMapIndex("a55","e");
		DBApp.createBitMapIndex("a55","f");
		DBApp.createBitMapIndex("a55","g");
		DBApp.createBitMapIndex("a55","h");
		tableTrace2 = DBApp.getFullTrace("a55");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("c5j", cols3);
		String tableTrace3 = DBApp.getFullTrace("c5j");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("c5j","a");
		DBApp.createBitMapIndex("c5j","b");
		DBApp.createBitMapIndex("c5j","c");
		DBApp.createBitMapIndex("c5j","d");
		DBApp.createBitMapIndex("c5j","e");
		DBApp.createBitMapIndex("c5j","f");
		DBApp.createBitMapIndex("c5j","g");
		tableTrace3 = DBApp.getFullTrace("c5j");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("x237", cols4);
		String tableTrace4 = DBApp.getFullTrace("x237");
		assertTrue("Full trace should report indexed columns.", tableTrace4.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace4.endsWith("[]"));
		DBApp.createBitMapIndex("x237","a");
		DBApp.createBitMapIndex("x237","b");
		DBApp.createBitMapIndex("x237","c");
		DBApp.createBitMapIndex("x237","d");
		DBApp.createBitMapIndex("x237","e");
		DBApp.createBitMapIndex("x237","f");
		DBApp.createBitMapIndex("x237","g");
		tableTrace4 = DBApp.getFullTrace("x237");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace4.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_53() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("hr7v5", cols0);
		String tableTrace0 = DBApp.getFullTrace("hr7v5");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("hr7v5","a");
		DBApp.createBitMapIndex("hr7v5","b");
		DBApp.createBitMapIndex("hr7v5","c");
		DBApp.createBitMapIndex("hr7v5","d");
		DBApp.createBitMapIndex("hr7v5","e");
		DBApp.createBitMapIndex("hr7v5","f");
		DBApp.createBitMapIndex("hr7v5","g");
		DBApp.createBitMapIndex("hr7v5","h");
		DBApp.createBitMapIndex("hr7v5","i");
		DBApp.createBitMapIndex("hr7v5","j");
		DBApp.createBitMapIndex("hr7v5","k");
		DBApp.createBitMapIndex("hr7v5","l");
		DBApp.createBitMapIndex("hr7v5","m");
		tableTrace0 = DBApp.getFullTrace("hr7v5");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("e5g", cols1);
		String tableTrace1 = DBApp.getFullTrace("e5g");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("e5g","a");
		DBApp.createBitMapIndex("e5g","b");
		DBApp.createBitMapIndex("e5g","c");
		DBApp.createBitMapIndex("e5g","d");
		DBApp.createBitMapIndex("e5g","e");
		DBApp.createBitMapIndex("e5g","f");
		DBApp.createBitMapIndex("e5g","g");
		tableTrace1 = DBApp.getFullTrace("e5g");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_54() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 20;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("gvb", cols0);
		String tableTrace0 = DBApp.getFullTrace("gvb");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("gvb","a");
		DBApp.createBitMapIndex("gvb","b");
		DBApp.createBitMapIndex("gvb","c");
		DBApp.createBitMapIndex("gvb","d");
		DBApp.createBitMapIndex("gvb","e");
		DBApp.createBitMapIndex("gvb","f");
		DBApp.createBitMapIndex("gvb","g");
		DBApp.createBitMapIndex("gvb","h");
		DBApp.createBitMapIndex("gvb","i");
		DBApp.createBitMapIndex("gvb","j");
		DBApp.createBitMapIndex("gvb","k");
		DBApp.createBitMapIndex("gvb","l");
		DBApp.createBitMapIndex("gvb","m");
		tableTrace0 = DBApp.getFullTrace("gvb");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("es6k", cols1);
		String tableTrace1 = DBApp.getFullTrace("es6k");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("es6k","a");
		DBApp.createBitMapIndex("es6k","b");
		DBApp.createBitMapIndex("es6k","c");
		DBApp.createBitMapIndex("es6k","d");
		DBApp.createBitMapIndex("es6k","e");
		DBApp.createBitMapIndex("es6k","f");
		DBApp.createBitMapIndex("es6k","g");
		tableTrace1 = DBApp.getFullTrace("es6k");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("i5g1", cols2);
		String tableTrace2 = DBApp.getFullTrace("i5g1");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("i5g1","a");
		DBApp.createBitMapIndex("i5g1","b");
		DBApp.createBitMapIndex("i5g1","c");
		DBApp.createBitMapIndex("i5g1","d");
		DBApp.createBitMapIndex("i5g1","e");
		DBApp.createBitMapIndex("i5g1","f");
		DBApp.createBitMapIndex("i5g1","g");
		DBApp.createBitMapIndex("i5g1","h");
		DBApp.createBitMapIndex("i5g1","i");
		DBApp.createBitMapIndex("i5g1","j");
		DBApp.createBitMapIndex("i5g1","k");
		DBApp.createBitMapIndex("i5g1","l");
		DBApp.createBitMapIndex("i5g1","m");
		DBApp.createBitMapIndex("i5g1","n");
		DBApp.createBitMapIndex("i5g1","o");
		DBApp.createBitMapIndex("i5g1","p");
		DBApp.createBitMapIndex("i5g1","q");
		DBApp.createBitMapIndex("i5g1","r");
		DBApp.createBitMapIndex("i5g1","s");
		DBApp.createBitMapIndex("i5g1","t");
		tableTrace2 = DBApp.getFullTrace("i5g1");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("xp4", cols3);
		String tableTrace3 = DBApp.getFullTrace("xp4");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("xp4","a");
		DBApp.createBitMapIndex("xp4","b");
		DBApp.createBitMapIndex("xp4","c");
		DBApp.createBitMapIndex("xp4","d");
		DBApp.createBitMapIndex("xp4","e");
		DBApp.createBitMapIndex("xp4","f");
		DBApp.createBitMapIndex("xp4","g");
		tableTrace3 = DBApp.getFullTrace("xp4");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("w89y", cols4);
		String tableTrace4 = DBApp.getFullTrace("w89y");
		assertTrue("Full trace should report indexed columns.", tableTrace4.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace4.endsWith("[]"));
		DBApp.createBitMapIndex("w89y","a");
		DBApp.createBitMapIndex("w89y","b");
		DBApp.createBitMapIndex("w89y","c");
		DBApp.createBitMapIndex("w89y","d");
		DBApp.createBitMapIndex("w89y","e");
		DBApp.createBitMapIndex("w89y","f");
		DBApp.createBitMapIndex("w89y","g");
		DBApp.createBitMapIndex("w89y","h");
		DBApp.createBitMapIndex("w89y","i");
		DBApp.createBitMapIndex("w89y","j");
		tableTrace4 = DBApp.getFullTrace("w89y");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace4.endsWith("[a, b, c, d, e, f, g, h, i, j]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_55() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o5", cols0);
		String tableTrace0 = DBApp.getFullTrace("o5");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("o5","a");
		DBApp.createBitMapIndex("o5","b");
		DBApp.createBitMapIndex("o5","c");
		DBApp.createBitMapIndex("o5","d");
		DBApp.createBitMapIndex("o5","e");
		DBApp.createBitMapIndex("o5","f");
		DBApp.createBitMapIndex("o5","g");
		tableTrace0 = DBApp.getFullTrace("o5");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_56() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("e8pq6", cols0);
		String tableTrace0 = DBApp.getFullTrace("e8pq6");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("e8pq6","a");
		DBApp.createBitMapIndex("e8pq6","b");
		DBApp.createBitMapIndex("e8pq6","c");
		DBApp.createBitMapIndex("e8pq6","d");
		DBApp.createBitMapIndex("e8pq6","e");
		DBApp.createBitMapIndex("e8pq6","f");
		DBApp.createBitMapIndex("e8pq6","g");
		DBApp.createBitMapIndex("e8pq6","h");
		DBApp.createBitMapIndex("e8pq6","i");
		DBApp.createBitMapIndex("e8pq6","j");
		DBApp.createBitMapIndex("e8pq6","k");
		DBApp.createBitMapIndex("e8pq6","l");
		DBApp.createBitMapIndex("e8pq6","m");
		DBApp.createBitMapIndex("e8pq6","n");
		DBApp.createBitMapIndex("e8pq6","o");
		DBApp.createBitMapIndex("e8pq6","p");
		DBApp.createBitMapIndex("e8pq6","q");
		DBApp.createBitMapIndex("e8pq6","r");
		DBApp.createBitMapIndex("e8pq6","s");
		tableTrace0 = DBApp.getFullTrace("e8pq6");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("lp5", cols1);
		String tableTrace1 = DBApp.getFullTrace("lp5");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("lp5","a");
		DBApp.createBitMapIndex("lp5","b");
		DBApp.createBitMapIndex("lp5","c");
		DBApp.createBitMapIndex("lp5","d");
		DBApp.createBitMapIndex("lp5","e");
		DBApp.createBitMapIndex("lp5","f");
		DBApp.createBitMapIndex("lp5","g");
		DBApp.createBitMapIndex("lp5","h");
		DBApp.createBitMapIndex("lp5","i");
		DBApp.createBitMapIndex("lp5","j");
		DBApp.createBitMapIndex("lp5","k");
		tableTrace1 = DBApp.getFullTrace("lp5");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q394b", cols2);
		String tableTrace2 = DBApp.getFullTrace("q394b");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("q394b","a");
		DBApp.createBitMapIndex("q394b","b");
		DBApp.createBitMapIndex("q394b","c");
		DBApp.createBitMapIndex("q394b","d");
		DBApp.createBitMapIndex("q394b","e");
		DBApp.createBitMapIndex("q394b","f");
		DBApp.createBitMapIndex("q394b","g");
		tableTrace2 = DBApp.getFullTrace("q394b");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("i35ph", cols3);
		String tableTrace3 = DBApp.getFullTrace("i35ph");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("i35ph","a");
		DBApp.createBitMapIndex("i35ph","b");
		DBApp.createBitMapIndex("i35ph","c");
		DBApp.createBitMapIndex("i35ph","d");
		DBApp.createBitMapIndex("i35ph","e");
		DBApp.createBitMapIndex("i35ph","f");
		DBApp.createBitMapIndex("i35ph","g");
		tableTrace3 = DBApp.getFullTrace("i35ph");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_57() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ye0i", cols0);
		String tableTrace0 = DBApp.getFullTrace("ye0i");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("ye0i","a");
		DBApp.createBitMapIndex("ye0i","b");
		DBApp.createBitMapIndex("ye0i","c");
		DBApp.createBitMapIndex("ye0i","d");
		DBApp.createBitMapIndex("ye0i","e");
		DBApp.createBitMapIndex("ye0i","f");
		DBApp.createBitMapIndex("ye0i","g");
		tableTrace0 = DBApp.getFullTrace("ye0i");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("d10q", cols1);
		String tableTrace1 = DBApp.getFullTrace("d10q");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("d10q","a");
		DBApp.createBitMapIndex("d10q","b");
		DBApp.createBitMapIndex("d10q","c");
		DBApp.createBitMapIndex("d10q","d");
		DBApp.createBitMapIndex("d10q","e");
		DBApp.createBitMapIndex("d10q","f");
		DBApp.createBitMapIndex("d10q","g");
		tableTrace1 = DBApp.getFullTrace("d10q");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("siwp7", cols2);
		String tableTrace2 = DBApp.getFullTrace("siwp7");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("siwp7","a");
		DBApp.createBitMapIndex("siwp7","b");
		DBApp.createBitMapIndex("siwp7","c");
		DBApp.createBitMapIndex("siwp7","d");
		DBApp.createBitMapIndex("siwp7","e");
		DBApp.createBitMapIndex("siwp7","f");
		DBApp.createBitMapIndex("siwp7","g");
		DBApp.createBitMapIndex("siwp7","h");
		DBApp.createBitMapIndex("siwp7","i");
		DBApp.createBitMapIndex("siwp7","j");
		DBApp.createBitMapIndex("siwp7","k");
		DBApp.createBitMapIndex("siwp7","l");
		DBApp.createBitMapIndex("siwp7","m");
		DBApp.createBitMapIndex("siwp7","n");
		DBApp.createBitMapIndex("siwp7","o");
		DBApp.createBitMapIndex("siwp7","p");
		DBApp.createBitMapIndex("siwp7","q");
		DBApp.createBitMapIndex("siwp7","r");
		DBApp.createBitMapIndex("siwp7","s");
		tableTrace2 = DBApp.getFullTrace("siwp7");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("els", cols3);
		String tableTrace3 = DBApp.getFullTrace("els");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("els","a");
		DBApp.createBitMapIndex("els","b");
		DBApp.createBitMapIndex("els","c");
		DBApp.createBitMapIndex("els","d");
		DBApp.createBitMapIndex("els","e");
		DBApp.createBitMapIndex("els","f");
		DBApp.createBitMapIndex("els","g");
		DBApp.createBitMapIndex("els","h");
		DBApp.createBitMapIndex("els","i");
		DBApp.createBitMapIndex("els","j");
		DBApp.createBitMapIndex("els","k");
		DBApp.createBitMapIndex("els","l");
		DBApp.createBitMapIndex("els","m");
		DBApp.createBitMapIndex("els","n");
		DBApp.createBitMapIndex("els","o");
		tableTrace3 = DBApp.getFullTrace("els");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_58() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("bb", cols0);
		String tableTrace0 = DBApp.getFullTrace("bb");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("bb","a");
		DBApp.createBitMapIndex("bb","b");
		DBApp.createBitMapIndex("bb","c");
		DBApp.createBitMapIndex("bb","d");
		DBApp.createBitMapIndex("bb","e");
		DBApp.createBitMapIndex("bb","f");
		DBApp.createBitMapIndex("bb","g");
		tableTrace0 = DBApp.getFullTrace("bb");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ll", cols1);
		String tableTrace1 = DBApp.getFullTrace("ll");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("ll","a");
		DBApp.createBitMapIndex("ll","b");
		DBApp.createBitMapIndex("ll","c");
		DBApp.createBitMapIndex("ll","d");
		DBApp.createBitMapIndex("ll","e");
		DBApp.createBitMapIndex("ll","f");
		DBApp.createBitMapIndex("ll","g");
		DBApp.createBitMapIndex("ll","h");
		DBApp.createBitMapIndex("ll","i");
		DBApp.createBitMapIndex("ll","j");
		tableTrace1 = DBApp.getFullTrace("ll");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_59() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("sj3", cols0);
		String tableTrace0 = DBApp.getFullTrace("sj3");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("sj3","a");
		DBApp.createBitMapIndex("sj3","b");
		DBApp.createBitMapIndex("sj3","c");
		DBApp.createBitMapIndex("sj3","d");
		DBApp.createBitMapIndex("sj3","e");
		DBApp.createBitMapIndex("sj3","f");
		DBApp.createBitMapIndex("sj3","g");
		DBApp.createBitMapIndex("sj3","h");
		tableTrace0 = DBApp.getFullTrace("sj3");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("t7b14", cols1);
		String tableTrace1 = DBApp.getFullTrace("t7b14");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("t7b14","a");
		DBApp.createBitMapIndex("t7b14","b");
		DBApp.createBitMapIndex("t7b14","c");
		DBApp.createBitMapIndex("t7b14","d");
		DBApp.createBitMapIndex("t7b14","e");
		DBApp.createBitMapIndex("t7b14","f");
		DBApp.createBitMapIndex("t7b14","g");
		DBApp.createBitMapIndex("t7b14","h");
		DBApp.createBitMapIndex("t7b14","i");
		DBApp.createBitMapIndex("t7b14","j");
		DBApp.createBitMapIndex("t7b14","k");
		DBApp.createBitMapIndex("t7b14","l");
		DBApp.createBitMapIndex("t7b14","m");
		DBApp.createBitMapIndex("t7b14","n");
		DBApp.createBitMapIndex("t7b14","o");
		DBApp.createBitMapIndex("t7b14","p");
		tableTrace1 = DBApp.getFullTrace("t7b14");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("krmv", cols2);
		String tableTrace2 = DBApp.getFullTrace("krmv");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("krmv","a");
		DBApp.createBitMapIndex("krmv","b");
		DBApp.createBitMapIndex("krmv","c");
		DBApp.createBitMapIndex("krmv","d");
		DBApp.createBitMapIndex("krmv","e");
		DBApp.createBitMapIndex("krmv","f");
		DBApp.createBitMapIndex("krmv","g");
		DBApp.createBitMapIndex("krmv","h");
		DBApp.createBitMapIndex("krmv","i");
		tableTrace2 = DBApp.getFullTrace("krmv");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("ig", cols3);
		String tableTrace3 = DBApp.getFullTrace("ig");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("ig","a");
		DBApp.createBitMapIndex("ig","b");
		DBApp.createBitMapIndex("ig","c");
		DBApp.createBitMapIndex("ig","d");
		DBApp.createBitMapIndex("ig","e");
		DBApp.createBitMapIndex("ig","f");
		DBApp.createBitMapIndex("ig","g");
		DBApp.createBitMapIndex("ig","h");
		DBApp.createBitMapIndex("ig","i");
		DBApp.createBitMapIndex("ig","j");
		DBApp.createBitMapIndex("ig","k");
		DBApp.createBitMapIndex("ig","l");
		tableTrace3 = DBApp.getFullTrace("ig");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l]"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("eptt", cols4);
		String tableTrace4 = DBApp.getFullTrace("eptt");
		assertTrue("Full trace should report indexed columns.", tableTrace4.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace4.endsWith("[]"));
		DBApp.createBitMapIndex("eptt","a");
		DBApp.createBitMapIndex("eptt","b");
		DBApp.createBitMapIndex("eptt","c");
		DBApp.createBitMapIndex("eptt","d");
		DBApp.createBitMapIndex("eptt","e");
		DBApp.createBitMapIndex("eptt","f");
		DBApp.createBitMapIndex("eptt","g");
		tableTrace4 = DBApp.getFullTrace("eptt");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace4.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_60() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("u27", cols0);
		String tableTrace0 = DBApp.getFullTrace("u27");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("u27","a");
		DBApp.createBitMapIndex("u27","b");
		DBApp.createBitMapIndex("u27","c");
		DBApp.createBitMapIndex("u27","d");
		DBApp.createBitMapIndex("u27","e");
		DBApp.createBitMapIndex("u27","f");
		DBApp.createBitMapIndex("u27","g");
		DBApp.createBitMapIndex("u27","h");
		DBApp.createBitMapIndex("u27","i");
		DBApp.createBitMapIndex("u27","j");
		DBApp.createBitMapIndex("u27","k");
		DBApp.createBitMapIndex("u27","l");
		DBApp.createBitMapIndex("u27","m");
		tableTrace0 = DBApp.getFullTrace("u27");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("p669l", cols1);
		String tableTrace1 = DBApp.getFullTrace("p669l");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("p669l","a");
		DBApp.createBitMapIndex("p669l","b");
		DBApp.createBitMapIndex("p669l","c");
		DBApp.createBitMapIndex("p669l","d");
		DBApp.createBitMapIndex("p669l","e");
		DBApp.createBitMapIndex("p669l","f");
		DBApp.createBitMapIndex("p669l","g");
		DBApp.createBitMapIndex("p669l","h");
		DBApp.createBitMapIndex("p669l","i");
		DBApp.createBitMapIndex("p669l","j");
		DBApp.createBitMapIndex("p669l","k");
		DBApp.createBitMapIndex("p669l","l");
		DBApp.createBitMapIndex("p669l","m");
		DBApp.createBitMapIndex("p669l","n");
		DBApp.createBitMapIndex("p669l","o");
		DBApp.createBitMapIndex("p669l","p");
		DBApp.createBitMapIndex("p669l","q");
		DBApp.createBitMapIndex("p669l","r");
		DBApp.createBitMapIndex("p669l","s");
		DBApp.createBitMapIndex("p669l","t");
		tableTrace1 = DBApp.getFullTrace("p669l");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("akb6t", cols2);
		String tableTrace2 = DBApp.getFullTrace("akb6t");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("akb6t","a");
		DBApp.createBitMapIndex("akb6t","b");
		DBApp.createBitMapIndex("akb6t","c");
		DBApp.createBitMapIndex("akb6t","d");
		DBApp.createBitMapIndex("akb6t","e");
		DBApp.createBitMapIndex("akb6t","f");
		DBApp.createBitMapIndex("akb6t","g");
		tableTrace2 = DBApp.getFullTrace("akb6t");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("s4k", cols3);
		String tableTrace3 = DBApp.getFullTrace("s4k");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("s4k","a");
		DBApp.createBitMapIndex("s4k","b");
		DBApp.createBitMapIndex("s4k","c");
		DBApp.createBitMapIndex("s4k","d");
		DBApp.createBitMapIndex("s4k","e");
		DBApp.createBitMapIndex("s4k","f");
		DBApp.createBitMapIndex("s4k","g");
		DBApp.createBitMapIndex("s4k","h");
		DBApp.createBitMapIndex("s4k","i");
		DBApp.createBitMapIndex("s4k","j");
		DBApp.createBitMapIndex("s4k","k");
		DBApp.createBitMapIndex("s4k","l");
		tableTrace3 = DBApp.getFullTrace("s4k");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l]"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("qgjc3", cols4);
		String tableTrace4 = DBApp.getFullTrace("qgjc3");
		assertTrue("Full trace should report indexed columns.", tableTrace4.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace4.endsWith("[]"));
		DBApp.createBitMapIndex("qgjc3","a");
		DBApp.createBitMapIndex("qgjc3","b");
		DBApp.createBitMapIndex("qgjc3","c");
		DBApp.createBitMapIndex("qgjc3","d");
		DBApp.createBitMapIndex("qgjc3","e");
		DBApp.createBitMapIndex("qgjc3","f");
		DBApp.createBitMapIndex("qgjc3","g");
		DBApp.createBitMapIndex("qgjc3","h");
		DBApp.createBitMapIndex("qgjc3","i");
		DBApp.createBitMapIndex("qgjc3","j");
		DBApp.createBitMapIndex("qgjc3","k");
		DBApp.createBitMapIndex("qgjc3","l");
		DBApp.createBitMapIndex("qgjc3","m");
		DBApp.createBitMapIndex("qgjc3","n");
		DBApp.createBitMapIndex("qgjc3","o");
		DBApp.createBitMapIndex("qgjc3","p");
		DBApp.createBitMapIndex("qgjc3","q");
		DBApp.createBitMapIndex("qgjc3","r");
		tableTrace4 = DBApp.getFullTrace("qgjc3");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace4.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_61() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("xd5s2", cols0);
		String tableTrace0 = DBApp.getFullTrace("xd5s2");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("xd5s2","a");
		DBApp.createBitMapIndex("xd5s2","b");
		DBApp.createBitMapIndex("xd5s2","c");
		DBApp.createBitMapIndex("xd5s2","d");
		DBApp.createBitMapIndex("xd5s2","e");
		DBApp.createBitMapIndex("xd5s2","f");
		DBApp.createBitMapIndex("xd5s2","g");
		tableTrace0 = DBApp.getFullTrace("xd5s2");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("wv166", cols1);
		String tableTrace1 = DBApp.getFullTrace("wv166");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("wv166","a");
		DBApp.createBitMapIndex("wv166","b");
		DBApp.createBitMapIndex("wv166","c");
		DBApp.createBitMapIndex("wv166","d");
		DBApp.createBitMapIndex("wv166","e");
		DBApp.createBitMapIndex("wv166","f");
		DBApp.createBitMapIndex("wv166","g");
		DBApp.createBitMapIndex("wv166","h");
		DBApp.createBitMapIndex("wv166","i");
		DBApp.createBitMapIndex("wv166","j");
		DBApp.createBitMapIndex("wv166","k");
		tableTrace1 = DBApp.getFullTrace("wv166");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ybq", cols2);
		String tableTrace2 = DBApp.getFullTrace("ybq");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("ybq","a");
		DBApp.createBitMapIndex("ybq","b");
		DBApp.createBitMapIndex("ybq","c");
		DBApp.createBitMapIndex("ybq","d");
		DBApp.createBitMapIndex("ybq","e");
		DBApp.createBitMapIndex("ybq","f");
		DBApp.createBitMapIndex("ybq","g");
		tableTrace2 = DBApp.getFullTrace("ybq");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("av", cols3);
		String tableTrace3 = DBApp.getFullTrace("av");
		assertTrue("Full trace should report indexed columns.", tableTrace3.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace3.endsWith("[]"));
		DBApp.createBitMapIndex("av","a");
		DBApp.createBitMapIndex("av","b");
		DBApp.createBitMapIndex("av","c");
		DBApp.createBitMapIndex("av","d");
		DBApp.createBitMapIndex("av","e");
		DBApp.createBitMapIndex("av","f");
		DBApp.createBitMapIndex("av","g");
		DBApp.createBitMapIndex("av","h");
		DBApp.createBitMapIndex("av","i");
		DBApp.createBitMapIndex("av","j");
		DBApp.createBitMapIndex("av","k");
		DBApp.createBitMapIndex("av","l");
		DBApp.createBitMapIndex("av","m");
		DBApp.createBitMapIndex("av","n");
		DBApp.createBitMapIndex("av","o");
		DBApp.createBitMapIndex("av","p");
		DBApp.createBitMapIndex("av","q");
		DBApp.createBitMapIndex("av","r");
		DBApp.createBitMapIndex("av","s");
		tableTrace3 = DBApp.getFullTrace("av");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace3.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_62() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("f6v", cols0);
		String tableTrace0 = DBApp.getFullTrace("f6v");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("f6v","a");
		DBApp.createBitMapIndex("f6v","b");
		DBApp.createBitMapIndex("f6v","c");
		DBApp.createBitMapIndex("f6v","d");
		DBApp.createBitMapIndex("f6v","e");
		DBApp.createBitMapIndex("f6v","f");
		DBApp.createBitMapIndex("f6v","g");
		DBApp.createBitMapIndex("f6v","h");
		DBApp.createBitMapIndex("f6v","i");
		DBApp.createBitMapIndex("f6v","j");
		DBApp.createBitMapIndex("f6v","k");
		tableTrace0 = DBApp.getFullTrace("f6v");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("l211j", cols1);
		String tableTrace1 = DBApp.getFullTrace("l211j");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("l211j","a");
		DBApp.createBitMapIndex("l211j","b");
		DBApp.createBitMapIndex("l211j","c");
		DBApp.createBitMapIndex("l211j","d");
		DBApp.createBitMapIndex("l211j","e");
		DBApp.createBitMapIndex("l211j","f");
		DBApp.createBitMapIndex("l211j","g");
		DBApp.createBitMapIndex("l211j","h");
		DBApp.createBitMapIndex("l211j","i");
		DBApp.createBitMapIndex("l211j","j");
		DBApp.createBitMapIndex("l211j","k");
		DBApp.createBitMapIndex("l211j","l");
		DBApp.createBitMapIndex("l211j","m");
		DBApp.createBitMapIndex("l211j","n");
		tableTrace1 = DBApp.getFullTrace("l211j");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("uuh17", cols2);
		String tableTrace2 = DBApp.getFullTrace("uuh17");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("uuh17","a");
		DBApp.createBitMapIndex("uuh17","b");
		DBApp.createBitMapIndex("uuh17","c");
		DBApp.createBitMapIndex("uuh17","d");
		DBApp.createBitMapIndex("uuh17","e");
		DBApp.createBitMapIndex("uuh17","f");
		DBApp.createBitMapIndex("uuh17","g");
		DBApp.createBitMapIndex("uuh17","h");
		DBApp.createBitMapIndex("uuh17","i");
		DBApp.createBitMapIndex("uuh17","j");
		DBApp.createBitMapIndex("uuh17","k");
		DBApp.createBitMapIndex("uuh17","l");
		DBApp.createBitMapIndex("uuh17","m");
		DBApp.createBitMapIndex("uuh17","n");
		DBApp.createBitMapIndex("uuh17","o");
		DBApp.createBitMapIndex("uuh17","p");
		DBApp.createBitMapIndex("uuh17","q");
		tableTrace2 = DBApp.getFullTrace("uuh17");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_63() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("lguiy", cols0);
		String tableTrace0 = DBApp.getFullTrace("lguiy");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("lguiy","a");
		DBApp.createBitMapIndex("lguiy","b");
		DBApp.createBitMapIndex("lguiy","c");
		DBApp.createBitMapIndex("lguiy","d");
		DBApp.createBitMapIndex("lguiy","e");
		DBApp.createBitMapIndex("lguiy","f");
		DBApp.createBitMapIndex("lguiy","g");
		DBApp.createBitMapIndex("lguiy","h");
		tableTrace0 = DBApp.getFullTrace("lguiy");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("d4i47", cols1);
		String tableTrace1 = DBApp.getFullTrace("d4i47");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("d4i47","a");
		DBApp.createBitMapIndex("d4i47","b");
		DBApp.createBitMapIndex("d4i47","c");
		DBApp.createBitMapIndex("d4i47","d");
		DBApp.createBitMapIndex("d4i47","e");
		DBApp.createBitMapIndex("d4i47","f");
		DBApp.createBitMapIndex("d4i47","g");
		tableTrace1 = DBApp.getFullTrace("d4i47");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("g8d2", cols2);
		String tableTrace2 = DBApp.getFullTrace("g8d2");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("g8d2","a");
		DBApp.createBitMapIndex("g8d2","b");
		DBApp.createBitMapIndex("g8d2","c");
		DBApp.createBitMapIndex("g8d2","d");
		DBApp.createBitMapIndex("g8d2","e");
		DBApp.createBitMapIndex("g8d2","f");
		DBApp.createBitMapIndex("g8d2","g");
		DBApp.createBitMapIndex("g8d2","h");
		DBApp.createBitMapIndex("g8d2","i");
		DBApp.createBitMapIndex("g8d2","j");
		DBApp.createBitMapIndex("g8d2","k");
		DBApp.createBitMapIndex("g8d2","l");
		DBApp.createBitMapIndex("g8d2","m");
		DBApp.createBitMapIndex("g8d2","n");
		DBApp.createBitMapIndex("g8d2","o");
		DBApp.createBitMapIndex("g8d2","p");
		DBApp.createBitMapIndex("g8d2","q");
		DBApp.createBitMapIndex("g8d2","r");
		DBApp.createBitMapIndex("g8d2","s");
		DBApp.createBitMapIndex("g8d2","t");
		tableTrace2 = DBApp.getFullTrace("g8d2");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_64() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("ey15", cols0);
		String tableTrace0 = DBApp.getFullTrace("ey15");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("ey15","a");
		DBApp.createBitMapIndex("ey15","b");
		DBApp.createBitMapIndex("ey15","c");
		DBApp.createBitMapIndex("ey15","d");
		DBApp.createBitMapIndex("ey15","e");
		DBApp.createBitMapIndex("ey15","f");
		DBApp.createBitMapIndex("ey15","g");
		DBApp.createBitMapIndex("ey15","h");
		DBApp.createBitMapIndex("ey15","i");
		DBApp.createBitMapIndex("ey15","j");
		DBApp.createBitMapIndex("ey15","k");
		DBApp.createBitMapIndex("ey15","l");
		DBApp.createBitMapIndex("ey15","m");
		tableTrace0 = DBApp.getFullTrace("ey15");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_65() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 50;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("akd", cols0);
		String tableTrace0 = DBApp.getFullTrace("akd");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("akd","a");
		DBApp.createBitMapIndex("akd","b");
		DBApp.createBitMapIndex("akd","c");
		DBApp.createBitMapIndex("akd","d");
		DBApp.createBitMapIndex("akd","e");
		DBApp.createBitMapIndex("akd","f");
		DBApp.createBitMapIndex("akd","g");
		DBApp.createBitMapIndex("akd","h");
		DBApp.createBitMapIndex("akd","i");
		DBApp.createBitMapIndex("akd","j");
		DBApp.createBitMapIndex("akd","k");
		DBApp.createBitMapIndex("akd","l");
		DBApp.createBitMapIndex("akd","m");
		DBApp.createBitMapIndex("akd","n");
		tableTrace0 = DBApp.getFullTrace("akd");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_66() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("ofq", cols0);
		String tableTrace0 = DBApp.getFullTrace("ofq");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("ofq","a");
		DBApp.createBitMapIndex("ofq","b");
		DBApp.createBitMapIndex("ofq","c");
		DBApp.createBitMapIndex("ofq","d");
		DBApp.createBitMapIndex("ofq","e");
		DBApp.createBitMapIndex("ofq","f");
		DBApp.createBitMapIndex("ofq","g");
		DBApp.createBitMapIndex("ofq","h");
		DBApp.createBitMapIndex("ofq","i");
		DBApp.createBitMapIndex("ofq","j");
		DBApp.createBitMapIndex("ofq","k");
		DBApp.createBitMapIndex("ofq","l");
		DBApp.createBitMapIndex("ofq","m");
		DBApp.createBitMapIndex("ofq","n");
		DBApp.createBitMapIndex("ofq","o");
		DBApp.createBitMapIndex("ofq","p");
		DBApp.createBitMapIndex("ofq","q");
		tableTrace0 = DBApp.getFullTrace("ofq");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("fb", cols1);
		String tableTrace1 = DBApp.getFullTrace("fb");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("fb","a");
		DBApp.createBitMapIndex("fb","b");
		DBApp.createBitMapIndex("fb","c");
		DBApp.createBitMapIndex("fb","d");
		DBApp.createBitMapIndex("fb","e");
		DBApp.createBitMapIndex("fb","f");
		DBApp.createBitMapIndex("fb","g");
		tableTrace1 = DBApp.getFullTrace("fb");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ibs", cols2);
		String tableTrace2 = DBApp.getFullTrace("ibs");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("ibs","a");
		DBApp.createBitMapIndex("ibs","b");
		DBApp.createBitMapIndex("ibs","c");
		DBApp.createBitMapIndex("ibs","d");
		DBApp.createBitMapIndex("ibs","e");
		DBApp.createBitMapIndex("ibs","f");
		DBApp.createBitMapIndex("ibs","g");
		tableTrace2 = DBApp.getFullTrace("ibs");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_67() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("kqk4q", cols0);
		String tableTrace0 = DBApp.getFullTrace("kqk4q");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("kqk4q","a");
		DBApp.createBitMapIndex("kqk4q","b");
		DBApp.createBitMapIndex("kqk4q","c");
		DBApp.createBitMapIndex("kqk4q","d");
		DBApp.createBitMapIndex("kqk4q","e");
		DBApp.createBitMapIndex("kqk4q","f");
		DBApp.createBitMapIndex("kqk4q","g");
		DBApp.createBitMapIndex("kqk4q","h");
		DBApp.createBitMapIndex("kqk4q","i");
		tableTrace0 = DBApp.getFullTrace("kqk4q");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("fbd8", cols1);
		String tableTrace1 = DBApp.getFullTrace("fbd8");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("fbd8","a");
		DBApp.createBitMapIndex("fbd8","b");
		DBApp.createBitMapIndex("fbd8","c");
		DBApp.createBitMapIndex("fbd8","d");
		DBApp.createBitMapIndex("fbd8","e");
		DBApp.createBitMapIndex("fbd8","f");
		DBApp.createBitMapIndex("fbd8","g");
		DBApp.createBitMapIndex("fbd8","h");
		DBApp.createBitMapIndex("fbd8","i");
		tableTrace1 = DBApp.getFullTrace("fbd8");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("j08f1", cols2);
		String tableTrace2 = DBApp.getFullTrace("j08f1");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("j08f1","a");
		DBApp.createBitMapIndex("j08f1","b");
		DBApp.createBitMapIndex("j08f1","c");
		DBApp.createBitMapIndex("j08f1","d");
		DBApp.createBitMapIndex("j08f1","e");
		DBApp.createBitMapIndex("j08f1","f");
		DBApp.createBitMapIndex("j08f1","g");
		DBApp.createBitMapIndex("j08f1","h");
		DBApp.createBitMapIndex("j08f1","i");
		DBApp.createBitMapIndex("j08f1","j");
		DBApp.createBitMapIndex("j08f1","k");
		DBApp.createBitMapIndex("j08f1","l");
		DBApp.createBitMapIndex("j08f1","m");
		DBApp.createBitMapIndex("j08f1","n");
		DBApp.createBitMapIndex("j08f1","o");
		DBApp.createBitMapIndex("j08f1","p");
		DBApp.createBitMapIndex("j08f1","q");
		tableTrace2 = DBApp.getFullTrace("j08f1");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_68() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("w09", cols0);
		String tableTrace0 = DBApp.getFullTrace("w09");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("w09","a");
		DBApp.createBitMapIndex("w09","b");
		DBApp.createBitMapIndex("w09","c");
		DBApp.createBitMapIndex("w09","d");
		DBApp.createBitMapIndex("w09","e");
		DBApp.createBitMapIndex("w09","f");
		DBApp.createBitMapIndex("w09","g");
		tableTrace0 = DBApp.getFullTrace("w09");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("l2e0", cols1);
		String tableTrace1 = DBApp.getFullTrace("l2e0");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("l2e0","a");
		DBApp.createBitMapIndex("l2e0","b");
		DBApp.createBitMapIndex("l2e0","c");
		DBApp.createBitMapIndex("l2e0","d");
		DBApp.createBitMapIndex("l2e0","e");
		DBApp.createBitMapIndex("l2e0","f");
		DBApp.createBitMapIndex("l2e0","g");
		DBApp.createBitMapIndex("l2e0","h");
		DBApp.createBitMapIndex("l2e0","i");
		DBApp.createBitMapIndex("l2e0","j");
		DBApp.createBitMapIndex("l2e0","k");
		DBApp.createBitMapIndex("l2e0","l");
		DBApp.createBitMapIndex("l2e0","m");
		DBApp.createBitMapIndex("l2e0","n");
		DBApp.createBitMapIndex("l2e0","o");
		DBApp.createBitMapIndex("l2e0","p");
		DBApp.createBitMapIndex("l2e0","q");
		tableTrace1 = DBApp.getFullTrace("l2e0");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("qg86", cols2);
		String tableTrace2 = DBApp.getFullTrace("qg86");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("qg86","a");
		DBApp.createBitMapIndex("qg86","b");
		DBApp.createBitMapIndex("qg86","c");
		DBApp.createBitMapIndex("qg86","d");
		DBApp.createBitMapIndex("qg86","e");
		DBApp.createBitMapIndex("qg86","f");
		DBApp.createBitMapIndex("qg86","g");
		DBApp.createBitMapIndex("qg86","h");
		DBApp.createBitMapIndex("qg86","i");
		DBApp.createBitMapIndex("qg86","j");
		DBApp.createBitMapIndex("qg86","k");
		DBApp.createBitMapIndex("qg86","l");
		DBApp.createBitMapIndex("qg86","m");
		DBApp.createBitMapIndex("qg86","n");
		tableTrace2 = DBApp.getFullTrace("qg86");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_69() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("b54", cols0);
		String tableTrace0 = DBApp.getFullTrace("b54");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("b54","a");
		DBApp.createBitMapIndex("b54","b");
		DBApp.createBitMapIndex("b54","c");
		DBApp.createBitMapIndex("b54","d");
		DBApp.createBitMapIndex("b54","e");
		DBApp.createBitMapIndex("b54","f");
		DBApp.createBitMapIndex("b54","g");
		DBApp.createBitMapIndex("b54","h");
		DBApp.createBitMapIndex("b54","i");
		tableTrace0 = DBApp.getFullTrace("b54");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("a93", cols1);
		String tableTrace1 = DBApp.getFullTrace("a93");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("a93","a");
		DBApp.createBitMapIndex("a93","b");
		DBApp.createBitMapIndex("a93","c");
		DBApp.createBitMapIndex("a93","d");
		DBApp.createBitMapIndex("a93","e");
		DBApp.createBitMapIndex("a93","f");
		DBApp.createBitMapIndex("a93","g");
		tableTrace1 = DBApp.getFullTrace("a93");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_70() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("pw87", cols0);
		String tableTrace0 = DBApp.getFullTrace("pw87");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("pw87","a");
		DBApp.createBitMapIndex("pw87","b");
		DBApp.createBitMapIndex("pw87","c");
		DBApp.createBitMapIndex("pw87","d");
		DBApp.createBitMapIndex("pw87","e");
		DBApp.createBitMapIndex("pw87","f");
		DBApp.createBitMapIndex("pw87","g");
		DBApp.createBitMapIndex("pw87","h");
		tableTrace0 = DBApp.getFullTrace("pw87");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("egr7e", cols1);
		String tableTrace1 = DBApp.getFullTrace("egr7e");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("egr7e","a");
		DBApp.createBitMapIndex("egr7e","b");
		DBApp.createBitMapIndex("egr7e","c");
		DBApp.createBitMapIndex("egr7e","d");
		DBApp.createBitMapIndex("egr7e","e");
		DBApp.createBitMapIndex("egr7e","f");
		DBApp.createBitMapIndex("egr7e","g");
		tableTrace1 = DBApp.getFullTrace("egr7e");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q42", cols2);
		String tableTrace2 = DBApp.getFullTrace("q42");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("q42","a");
		DBApp.createBitMapIndex("q42","b");
		DBApp.createBitMapIndex("q42","c");
		DBApp.createBitMapIndex("q42","d");
		DBApp.createBitMapIndex("q42","e");
		DBApp.createBitMapIndex("q42","f");
		DBApp.createBitMapIndex("q42","g");
		tableTrace2 = DBApp.getFullTrace("q42");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_71() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("u3g", cols0);
		String tableTrace0 = DBApp.getFullTrace("u3g");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("u3g","a");
		DBApp.createBitMapIndex("u3g","b");
		DBApp.createBitMapIndex("u3g","c");
		DBApp.createBitMapIndex("u3g","d");
		DBApp.createBitMapIndex("u3g","e");
		DBApp.createBitMapIndex("u3g","f");
		DBApp.createBitMapIndex("u3g","g");
		DBApp.createBitMapIndex("u3g","h");
		DBApp.createBitMapIndex("u3g","i");
		DBApp.createBitMapIndex("u3g","j");
		DBApp.createBitMapIndex("u3g","k");
		DBApp.createBitMapIndex("u3g","l");
		DBApp.createBitMapIndex("u3g","m");
		DBApp.createBitMapIndex("u3g","n");
		DBApp.createBitMapIndex("u3g","o");
		DBApp.createBitMapIndex("u3g","p");
		DBApp.createBitMapIndex("u3g","q");
		DBApp.createBitMapIndex("u3g","r");
		DBApp.createBitMapIndex("u3g","s");
		DBApp.createBitMapIndex("u3g","t");
		tableTrace0 = DBApp.getFullTrace("u3g");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_72() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("xlsa", cols0);
		String tableTrace0 = DBApp.getFullTrace("xlsa");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("xlsa","a");
		DBApp.createBitMapIndex("xlsa","b");
		DBApp.createBitMapIndex("xlsa","c");
		DBApp.createBitMapIndex("xlsa","d");
		DBApp.createBitMapIndex("xlsa","e");
		DBApp.createBitMapIndex("xlsa","f");
		DBApp.createBitMapIndex("xlsa","g");
		DBApp.createBitMapIndex("xlsa","h");
		tableTrace0 = DBApp.getFullTrace("xlsa");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("ytp", cols1);
		String tableTrace1 = DBApp.getFullTrace("ytp");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("ytp","a");
		DBApp.createBitMapIndex("ytp","b");
		DBApp.createBitMapIndex("ytp","c");
		DBApp.createBitMapIndex("ytp","d");
		DBApp.createBitMapIndex("ytp","e");
		DBApp.createBitMapIndex("ytp","f");
		DBApp.createBitMapIndex("ytp","g");
		DBApp.createBitMapIndex("ytp","h");
		DBApp.createBitMapIndex("ytp","i");
		DBApp.createBitMapIndex("ytp","j");
		tableTrace1 = DBApp.getFullTrace("ytp");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g, h, i, j]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("kch4", cols2);
		String tableTrace2 = DBApp.getFullTrace("kch4");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("kch4","a");
		DBApp.createBitMapIndex("kch4","b");
		DBApp.createBitMapIndex("kch4","c");
		DBApp.createBitMapIndex("kch4","d");
		DBApp.createBitMapIndex("kch4","e");
		DBApp.createBitMapIndex("kch4","f");
		DBApp.createBitMapIndex("kch4","g");
		tableTrace2 = DBApp.getFullTrace("kch4");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_73() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("a71", cols0);
		String tableTrace0 = DBApp.getFullTrace("a71");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("a71","a");
		DBApp.createBitMapIndex("a71","b");
		DBApp.createBitMapIndex("a71","c");
		DBApp.createBitMapIndex("a71","d");
		DBApp.createBitMapIndex("a71","e");
		DBApp.createBitMapIndex("a71","f");
		DBApp.createBitMapIndex("a71","g");
		DBApp.createBitMapIndex("a71","h");
		DBApp.createBitMapIndex("a71","i");
		DBApp.createBitMapIndex("a71","j");
		DBApp.createBitMapIndex("a71","k");
		DBApp.createBitMapIndex("a71","l");
		DBApp.createBitMapIndex("a71","m");
		DBApp.createBitMapIndex("a71","n");
		DBApp.createBitMapIndex("a71","o");
		DBApp.createBitMapIndex("a71","p");
		tableTrace0 = DBApp.getFullTrace("a71");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("tm89o", cols1);
		String tableTrace1 = DBApp.getFullTrace("tm89o");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("tm89o","a");
		DBApp.createBitMapIndex("tm89o","b");
		DBApp.createBitMapIndex("tm89o","c");
		DBApp.createBitMapIndex("tm89o","d");
		DBApp.createBitMapIndex("tm89o","e");
		DBApp.createBitMapIndex("tm89o","f");
		DBApp.createBitMapIndex("tm89o","g");
		tableTrace1 = DBApp.getFullTrace("tm89o");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_74() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("f96", cols0);
		String tableTrace0 = DBApp.getFullTrace("f96");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("f96","a");
		DBApp.createBitMapIndex("f96","b");
		DBApp.createBitMapIndex("f96","c");
		DBApp.createBitMapIndex("f96","d");
		DBApp.createBitMapIndex("f96","e");
		DBApp.createBitMapIndex("f96","f");
		DBApp.createBitMapIndex("f96","g");
		tableTrace0 = DBApp.getFullTrace("f96");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("p08", cols1);
		String tableTrace1 = DBApp.getFullTrace("p08");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("p08","a");
		DBApp.createBitMapIndex("p08","b");
		DBApp.createBitMapIndex("p08","c");
		DBApp.createBitMapIndex("p08","d");
		DBApp.createBitMapIndex("p08","e");
		DBApp.createBitMapIndex("p08","f");
		DBApp.createBitMapIndex("p08","g");
		tableTrace1 = DBApp.getFullTrace("p08");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_75() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("dkpiz", cols0);
		String tableTrace0 = DBApp.getFullTrace("dkpiz");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("dkpiz","a");
		DBApp.createBitMapIndex("dkpiz","b");
		DBApp.createBitMapIndex("dkpiz","c");
		DBApp.createBitMapIndex("dkpiz","d");
		DBApp.createBitMapIndex("dkpiz","e");
		DBApp.createBitMapIndex("dkpiz","f");
		DBApp.createBitMapIndex("dkpiz","g");
		DBApp.createBitMapIndex("dkpiz","h");
		DBApp.createBitMapIndex("dkpiz","i");
		DBApp.createBitMapIndex("dkpiz","j");
		DBApp.createBitMapIndex("dkpiz","k");
		DBApp.createBitMapIndex("dkpiz","l");
		DBApp.createBitMapIndex("dkpiz","m");
		DBApp.createBitMapIndex("dkpiz","n");
		DBApp.createBitMapIndex("dkpiz","o");
		tableTrace0 = DBApp.getFullTrace("dkpiz");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("jy", cols1);
		String tableTrace1 = DBApp.getFullTrace("jy");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("jy","a");
		DBApp.createBitMapIndex("jy","b");
		DBApp.createBitMapIndex("jy","c");
		DBApp.createBitMapIndex("jy","d");
		DBApp.createBitMapIndex("jy","e");
		DBApp.createBitMapIndex("jy","f");
		DBApp.createBitMapIndex("jy","g");
		tableTrace1 = DBApp.getFullTrace("jy");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mgvjy", cols2);
		String tableTrace2 = DBApp.getFullTrace("mgvjy");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("mgvjy","a");
		DBApp.createBitMapIndex("mgvjy","b");
		DBApp.createBitMapIndex("mgvjy","c");
		DBApp.createBitMapIndex("mgvjy","d");
		DBApp.createBitMapIndex("mgvjy","e");
		DBApp.createBitMapIndex("mgvjy","f");
		DBApp.createBitMapIndex("mgvjy","g");
		tableTrace2 = DBApp.getFullTrace("mgvjy");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void CreationIndexFullTrace_76() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 100;
		String[] cols0 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("z1yt5", cols0);
		String tableTrace0 = DBApp.getFullTrace("z1yt5");
		assertTrue("Full trace should report indexed columns.", tableTrace0.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace0.endsWith("[]"));
		DBApp.createBitMapIndex("z1yt5","a");
		DBApp.createBitMapIndex("z1yt5","b");
		DBApp.createBitMapIndex("z1yt5","c");
		DBApp.createBitMapIndex("z1yt5","d");
		DBApp.createBitMapIndex("z1yt5","e");
		DBApp.createBitMapIndex("z1yt5","f");
		DBApp.createBitMapIndex("z1yt5","g");
		DBApp.createBitMapIndex("z1yt5","h");
		tableTrace0 = DBApp.getFullTrace("z1yt5");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace0.endsWith("[a, b, c, d, e, f, g, h]"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("bx8o", cols1);
		String tableTrace1 = DBApp.getFullTrace("bx8o");
		assertTrue("Full trace should report indexed columns.", tableTrace1.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace1.endsWith("[]"));
		DBApp.createBitMapIndex("bx8o","a");
		DBApp.createBitMapIndex("bx8o","b");
		DBApp.createBitMapIndex("bx8o","c");
		DBApp.createBitMapIndex("bx8o","d");
		DBApp.createBitMapIndex("bx8o","e");
		DBApp.createBitMapIndex("bx8o","f");
		DBApp.createBitMapIndex("bx8o","g");
		tableTrace1 = DBApp.getFullTrace("bx8o");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace1.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("i50q", cols2);
		String tableTrace2 = DBApp.getFullTrace("i50q");
		assertTrue("Full trace should report indexed columns.", tableTrace2.contains("Indexed Columns: "));
		assertTrue("Full trace should report empty list when reporting indexed columns and non are.", tableTrace2.endsWith("[]"));
		DBApp.createBitMapIndex("i50q","a");
		DBApp.createBitMapIndex("i50q","b");
		DBApp.createBitMapIndex("i50q","c");
		DBApp.createBitMapIndex("i50q","d");
		DBApp.createBitMapIndex("i50q","e");
		DBApp.createBitMapIndex("i50q","f");
		DBApp.createBitMapIndex("i50q","g");
		tableTrace2 = DBApp.getFullTrace("i50q");
		assertTrue("Full trace should report the corect list when reporting indexed columns and all are.", tableTrace2.endsWith("[a, b, c, d, e, f, g]"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_0() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("a3", cols0);
		//To be indexed: [c, d, f, j, k], to be queried: [c0, d1, f1, j3, k11].
		String [][] records_a3 = new String[38][cols0.length];
		for(int i=0;i<38;i++)
		{
			records_a3[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_a3[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("a3", records_a3[i]);
		}
		DBApp.createBitMapIndex("a3","c");
		DBApp.createBitMapIndex("a3","d");
		DBApp.createBitMapIndex("a3","f");
		DBApp.createBitMapIndex("a3","j");
		DBApp.createBitMapIndex("a3","k");
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
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a3", "c", "c0"), ("10010010010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a3", "d", "d1"), ("01000100010001000100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a3", "f", "f1"), ("01000001000001000001000001000001000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("a3", "j", "j3"), ("00010000000001000000000100000000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("a3", "k", "k11"), ("00000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("u9r", cols1);
		//To be indexed: [b, d, e, f, h, k, m, n, o, p], to be queried: [b0, d2, e3, f3, h2, k4, m2, n8, o7, p16].
		String [][] records_u9r = new String[48][cols1.length];
		for(int i=0;i<48;i++)
		{
			records_u9r[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_u9r[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("u9r", records_u9r[i]);
		}
		DBApp.createBitMapIndex("u9r","b");
		DBApp.createBitMapIndex("u9r","d");
		DBApp.createBitMapIndex("u9r","e");
		DBApp.createBitMapIndex("u9r","f");
		DBApp.createBitMapIndex("u9r","h");
		DBApp.createBitMapIndex("u9r","k");
		DBApp.createBitMapIndex("u9r","m");
		DBApp.createBitMapIndex("u9r","n");
		DBApp.createBitMapIndex("u9r","o");
		DBApp.createBitMapIndex("u9r","p");
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
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8, n5, o2, p15]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u9r", "b", "b0"), ("101010101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u9r", "d", "d2"), ("001000100010001000100010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u9r", "e", "e3"), ("000100001000010000100001000010000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u9r", "f", "f3"), ("000100000100000100000100000100000100000100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u9r", "h", "h2"), ("001000000010000000100000001000000010000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u9r", "k", "k4"), ("000010000000000100000000001000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u9r", "m", "m2"), ("001000000000000100000000000010000000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u9r", "n", "n8"), ("000000001000000000000010000000000000100000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("u9r", "o", "o7"), ("000000010000000000000010000000000000010000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("u9r", "p", "p16"), ("000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("c889", cols2);
		//To be indexed: [b, c, d, f, g, j, m, n, p], to be queried: [b0, c1, d1, f2, g5, j4, m3, n9, p16].
		String [][] records_c889 = new String[12][cols2.length];
		for(int i=0;i<12;i++)
		{
			records_c889[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_c889[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("c889", records_c889[i]);
		}
		DBApp.createBitMapIndex("c889","b");
		DBApp.createBitMapIndex("c889","c");
		DBApp.createBitMapIndex("c889","d");
		DBApp.createBitMapIndex("c889","f");
		DBApp.createBitMapIndex("c889","g");
		DBApp.createBitMapIndex("c889","j");
		DBApp.createBitMapIndex("c889","m");
		DBApp.createBitMapIndex("c889","n");
		DBApp.createBitMapIndex("c889","p");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c889", "b", "b0"), ("101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c889", "c", "c1"), ("010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c889", "d", "d1"), ("010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c889", "f", "f2"), ("001000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c889", "g", "g5"), ("000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c889", "j", "j4"), ("000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c889", "m", "m3"), ("000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c889", "n", "n9"), ("000000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("c889", "p", "p16"), ("000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("n8", cols3);
		//To be indexed: [c, e, g, j, k, o, r], to be queried: [c1, e1, g3, j5, k4, o4, r18].
		String [][] records_n8 = new String[43][cols3.length];
		for(int i=0;i<43;i++)
		{
			records_n8[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_n8[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("n8", records_n8[i]);
		}
		DBApp.createBitMapIndex("n8","c");
		DBApp.createBitMapIndex("n8","e");
		DBApp.createBitMapIndex("n8","g");
		DBApp.createBitMapIndex("n8","j");
		DBApp.createBitMapIndex("n8","k");
		DBApp.createBitMapIndex("n8","o");
		DBApp.createBitMapIndex("n8","r");
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
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10, p8, q6, r4]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11, p9, q7, r5]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0, o12, p10, q8, r6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n8", "c", "c1"), ("0100100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n8", "e", "e1"), ("0100001000010000100001000010000100001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n8", "g", "g3"), ("0001000000100000010000001000000100000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n8", "j", "j5"), ("0000010000000001000000000100000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n8", "k", "k4"), ("0000100000000001000000000010000000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n8", "o", "o4"), ("0000100000000000000100000000000000100000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("n8", "r", "r18"), ("0000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("o6", cols4);
		//To be indexed: [b, e, f, g], to be queried: [b0, e0, f3, g7].
		String [][] records_o6 = new String[5][cols4.length];
		for(int i=0;i<5;i++)
		{
			records_o6[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_o6[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("o6", records_o6[i]);
		}
		DBApp.createBitMapIndex("o6","b");
		DBApp.createBitMapIndex("o6","e");
		DBApp.createBitMapIndex("o6","f");
		DBApp.createBitMapIndex("o6","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o6", "b", "b0"), ("10101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o6", "e", "e0"), ("10000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o6", "f", "f3"), ("00010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("o6", "g", "g7"), ("00000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_1() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("z50", cols0);
		//To be indexed: [b, d, i, k, l, m, n, o, r, s], to be queried: [b0, d1, i5, k8, l6, m6, n6, o3, r14, s19].
		String [][] records_z50 = new String[38][cols0.length];
		for(int i=0;i<38;i++)
		{
			records_z50[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_z50[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("z50", records_z50[i]);
		}
		DBApp.createBitMapIndex("z50","b");
		DBApp.createBitMapIndex("z50","d");
		DBApp.createBitMapIndex("z50","i");
		DBApp.createBitMapIndex("z50","k");
		DBApp.createBitMapIndex("z50","l");
		DBApp.createBitMapIndex("z50","m");
		DBApp.createBitMapIndex("z50","n");
		DBApp.createBitMapIndex("z50","o");
		DBApp.createBitMapIndex("z50","r");
		DBApp.createBitMapIndex("z50","s");
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
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9, o7, p5, q3, r1, s18]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z50", "b", "b0"), ("10101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z50", "d", "d1"), ("01000100010001000100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z50", "i", "i5"), ("00000100000000100000000100000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z50", "k", "k8"), ("00000000100000000001000000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z50", "l", "l6"), ("00000010000000000010000000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z50", "m", "m6"), ("00000010000000000001000000000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z50", "n", "n6"), ("00000010000000000000100000000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z50", "o", "o3"), ("00010000000000000010000000000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z50", "r", "r14"), ("00000000000000100000000000000000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("z50", "s", "s19"), ("00000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("e21", cols1);
		//To be indexed: [b, c, e, f, g, i, l], to be queried: [b0, c0, e2, f0, g0, i1, l12].
		String [][] records_e21 = new String[29][cols1.length];
		for(int i=0;i<29;i++)
		{
			records_e21[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_e21[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("e21", records_e21[i]);
		}
		DBApp.createBitMapIndex("e21","b");
		DBApp.createBitMapIndex("e21","c");
		DBApp.createBitMapIndex("e21","e");
		DBApp.createBitMapIndex("e21","f");
		DBApp.createBitMapIndex("e21","g");
		DBApp.createBitMapIndex("e21","i");
		DBApp.createBitMapIndex("e21","l");
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
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e21", "b", "b0"), ("10101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e21", "c", "c0"), ("10010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e21", "e", "e2"), ("00100001000010000100001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e21", "f", "f0"), ("10000010000010000010000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e21", "g", "g0"), ("10000001000000100000010000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e21", "i", "i1"), ("01000000001000000001000000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("e21", "l", "l12"), ("00000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("hp75", cols2);
		//To be indexed: [d, e, h, j, l, n, o, q, s], to be queried: [d1, e3, h1, j4, l7, n12, o9, q5, s19].
		String [][] records_hp75 = new String[9][cols2.length];
		for(int i=0;i<9;i++)
		{
			records_hp75[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_hp75[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("hp75", records_hp75[i]);
		}
		DBApp.createBitMapIndex("hp75","d");
		DBApp.createBitMapIndex("hp75","e");
		DBApp.createBitMapIndex("hp75","h");
		DBApp.createBitMapIndex("hp75","j");
		DBApp.createBitMapIndex("hp75","l");
		DBApp.createBitMapIndex("hp75","n");
		DBApp.createBitMapIndex("hp75","o");
		DBApp.createBitMapIndex("hp75","q");
		DBApp.createBitMapIndex("hp75","s");
		//first 9 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5, s5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6, s6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7, r7, s7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8, r8, s8]
		//last 9 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5, s5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6, m6, n6, o6, p6, q6, r6, s6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7, m7, n7, o7, p7, q7, r7, s7]
		//record8: [a8, b0, c2, d0, e3, f2, g1, h0, i8, j8, k8, l8, m8, n8, o8, p8, q8, r8, s8]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hp75", "d", "d1"), ("010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hp75", "e", "e3"), ("000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hp75", "h", "h1"), ("010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hp75", "j", "j4"), ("000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hp75", "l", "l7"), ("000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hp75", "n", "n12"), ("000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hp75", "o", "o9"), ("000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hp75", "q", "q5"), ("000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("hp75", "s", "s19"), ("000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("b8qri", cols3);
		//To be indexed: [b, f, o, q, r, s], to be queried: [b0, f1, o5, q13, r5, s19].
		String [][] records_b8qri = new String[31][cols3.length];
		for(int i=0;i<31;i++)
		{
			records_b8qri[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_b8qri[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("b8qri", records_b8qri[i]);
		}
		DBApp.createBitMapIndex("b8qri","b");
		DBApp.createBitMapIndex("b8qri","f");
		DBApp.createBitMapIndex("b8qri","o");
		DBApp.createBitMapIndex("b8qri","q");
		DBApp.createBitMapIndex("b8qri","r");
		DBApp.createBitMapIndex("b8qri","s");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b8qri", "b", "b0"), ("1010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b8qri", "f", "f1"), ("0100000100000100000100000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b8qri", "o", "o5"), ("0000010000000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b8qri", "q", "q13"), ("0000000000000100000000000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b8qri", "r", "r5"), ("0000010000000000000000010000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("b8qri", "s", "s19"), ("0000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("p6", cols4);
		//To be indexed: [e, g, h, i], to be queried: [e0, g3, h0, i9].
		String [][] records_p6 = new String[24][cols4.length];
		for(int i=0;i<24;i++)
		{
			records_p6[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_p6[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("p6", records_p6[i]);
		}
		DBApp.createBitMapIndex("p6","e");
		DBApp.createBitMapIndex("p6","g");
		DBApp.createBitMapIndex("p6","h");
		DBApp.createBitMapIndex("p6","i");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6", "e", "e0"), ("100001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6", "g", "g3"), ("000100000010000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6", "h", "h0"), ("100000001000000010000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("p6", "i", "i9"), ("000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_2() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("po07q", cols0);
		//To be indexed: [b, c, d, e, f, g, n, o, p, q], to be queried: [b0, c1, d1, e2, f0, g4, n6, o11, p0, q17].
		String [][] records_po07q = new String[5][cols0.length];
		for(int i=0;i<5;i++)
		{
			records_po07q[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_po07q[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("po07q", records_po07q[i]);
		}
		DBApp.createBitMapIndex("po07q","b");
		DBApp.createBitMapIndex("po07q","c");
		DBApp.createBitMapIndex("po07q","d");
		DBApp.createBitMapIndex("po07q","e");
		DBApp.createBitMapIndex("po07q","f");
		DBApp.createBitMapIndex("po07q","g");
		DBApp.createBitMapIndex("po07q","n");
		DBApp.createBitMapIndex("po07q","o");
		DBApp.createBitMapIndex("po07q","p");
		DBApp.createBitMapIndex("po07q","q");
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//last 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("po07q", "b", "b0"), ("10101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("po07q", "c", "c1"), ("01001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("po07q", "d", "d1"), ("01000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("po07q", "e", "e2"), ("00100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("po07q", "f", "f0"), ("10000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("po07q", "g", "g4"), ("00001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("po07q", "n", "n6"), ("00000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("po07q", "o", "o11"), ("00000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("po07q", "p", "p0"), ("10000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("po07q", "q", "q17"), ("00000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("z0s5i", cols1);
		//To be indexed: [c, f, m], to be queried: [c1, f2, m13].
		String [][] records_z0s5i = new String[24][cols1.length];
		for(int i=0;i<24;i++)
		{
			records_z0s5i[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_z0s5i[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("z0s5i", records_z0s5i[i]);
		}
		DBApp.createBitMapIndex("z0s5i","c");
		DBApp.createBitMapIndex("z0s5i","f");
		DBApp.createBitMapIndex("z0s5i","m");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z0s5i", "c", "c1"), ("010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z0s5i", "f", "f2"), ("001000001000001000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("z0s5i", "m", "m13"), ("000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("f0or", cols2);
		//To be indexed: [b, d, e, f, h, i, k, l, n, o, r], to be queried: [b0, d2, e2, f3, h3, i3, k7, l0, n9, o4, r18].
		String [][] records_f0or = new String[24][cols2.length];
		for(int i=0;i<24;i++)
		{
			records_f0or[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_f0or[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("f0or", records_f0or[i]);
		}
		DBApp.createBitMapIndex("f0or","b");
		DBApp.createBitMapIndex("f0or","d");
		DBApp.createBitMapIndex("f0or","e");
		DBApp.createBitMapIndex("f0or","f");
		DBApp.createBitMapIndex("f0or","h");
		DBApp.createBitMapIndex("f0or","i");
		DBApp.createBitMapIndex("f0or","k");
		DBApp.createBitMapIndex("f0or","l");
		DBApp.createBitMapIndex("f0or","n");
		DBApp.createBitMapIndex("f0or","o");
		DBApp.createBitMapIndex("f0or","r");
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
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7, p6, q5, r4]
		//record23: [a23, b1, c2, d3, e3, f5, g2, h7, i5, j3, k1, l11, m10, n9, o8, p7, q6, r5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f0or", "b", "b0"), ("101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f0or", "d", "d2"), ("001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f0or", "e", "e2"), ("001000010000100001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f0or", "f", "f3"), ("000100000100000100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f0or", "h", "h3"), ("000100000001000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f0or", "i", "i3"), ("000100000000100000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f0or", "k", "k7"), ("000000010000000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f0or", "l", "l0"), ("100000000000100000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f0or", "n", "n9"), ("000000000100000000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f0or", "o", "o4"), ("000010000000000000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("f0or", "r", "r18"), ("000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("xbrip", cols3);
		//To be indexed: [b, c, e, g], to be queried: [b0, c0, e3, g7].
		String [][] records_xbrip = new String[41][cols3.length];
		for(int i=0;i<41;i++)
		{
			records_xbrip[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_xbrip[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("xbrip", records_xbrip[i]);
		}
		DBApp.createBitMapIndex("xbrip","b");
		DBApp.createBitMapIndex("xbrip","c");
		DBApp.createBitMapIndex("xbrip","e");
		DBApp.createBitMapIndex("xbrip","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xbrip", "b", "b0"), ("10101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xbrip", "c", "c0"), ("10010010010010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("xbrip", "e", "e3"), ("00010000100001000010000100001000010000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("xbrip", "g", "g7"), ("00000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_3() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ljf", cols0);
		//To be indexed: [c, d, f, g], to be queried: [c1, d2, f2, g7].
		String [][] records_ljf = new String[40][cols0.length];
		for(int i=0;i<40;i++)
		{
			records_ljf[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ljf[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ljf", records_ljf[i]);
		}
		DBApp.createBitMapIndex("ljf","c");
		DBApp.createBitMapIndex("ljf","d");
		DBApp.createBitMapIndex("ljf","f");
		DBApp.createBitMapIndex("ljf","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ljf", "c", "c1"), ("0100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ljf", "d", "d2"), ("0010001000100010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ljf", "f", "f2"), ("0010000010000010000010000010000010000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ljf", "g", "g7"), ("0000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("c6p", cols1);
		//To be indexed: [b, d, e, g, i, j, k, l, n, p, q], to be queried: [b0, d1, e0, g0, i0, j6, k6, l8, n5, p2, q17].
		String [][] records_c6p = new String[40][cols1.length];
		for(int i=0;i<40;i++)
		{
			records_c6p[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_c6p[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("c6p", records_c6p[i]);
		}
		DBApp.createBitMapIndex("c6p","b");
		DBApp.createBitMapIndex("c6p","d");
		DBApp.createBitMapIndex("c6p","e");
		DBApp.createBitMapIndex("c6p","g");
		DBApp.createBitMapIndex("c6p","i");
		DBApp.createBitMapIndex("c6p","j");
		DBApp.createBitMapIndex("c6p","k");
		DBApp.createBitMapIndex("c6p","l");
		DBApp.createBitMapIndex("c6p","n");
		DBApp.createBitMapIndex("c6p","p");
		DBApp.createBitMapIndex("c6p","q");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c6p", "b", "b0"), ("1010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c6p", "d", "d1"), ("0100010001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c6p", "e", "e0"), ("1000010000100001000010000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c6p", "g", "g0"), ("1000000100000010000001000000100000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c6p", "i", "i0"), ("1000000001000000001000000001000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c6p", "j", "j6"), ("0000001000000000100000000010000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c6p", "k", "k6"), ("0000001000000000010000000000100000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c6p", "l", "l8"), ("0000000010000000000010000000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c6p", "n", "n5"), ("0000010000000000000100000000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("c6p", "p", "p2"), ("0010000000000000001000000000000000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("c6p", "q", "q17"), ("0000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m455d", cols2);
		//To be indexed: [d, e, f, g], to be queried: [d2, e1, f4, g7].
		String [][] records_m455d = new String[2][cols2.length];
		for(int i=0;i<2;i++)
		{
			records_m455d[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_m455d[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("m455d", records_m455d[i]);
		}
		DBApp.createBitMapIndex("m455d","d");
		DBApp.createBitMapIndex("m455d","e");
		DBApp.createBitMapIndex("m455d","f");
		DBApp.createBitMapIndex("m455d","g");
		//first 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		//last 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0]
		//record1: [a1, b1, c1, d1, e1, f1, g1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m455d", "d", "d2"), ("00"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m455d", "e", "e1"), ("01"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m455d", "f", "f4"), ("00"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("m455d", "g", "g7"), ("00"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_4() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("oam", cols0);
		//To be indexed: [b, c, d, g, h, i, j, k, m], to be queried: [b0, c1, d0, g3, h0, i1, j0, k8, m13].
		String [][] records_oam = new String[43][cols0.length];
		for(int i=0;i<43;i++)
		{
			records_oam[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_oam[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("oam", records_oam[i]);
		}
		DBApp.createBitMapIndex("oam","b");
		DBApp.createBitMapIndex("oam","c");
		DBApp.createBitMapIndex("oam","d");
		DBApp.createBitMapIndex("oam","g");
		DBApp.createBitMapIndex("oam","h");
		DBApp.createBitMapIndex("oam","i");
		DBApp.createBitMapIndex("oam","j");
		DBApp.createBitMapIndex("oam","k");
		DBApp.createBitMapIndex("oam","m");
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
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oam", "b", "b0"), ("1010101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oam", "c", "c1"), ("0100100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oam", "d", "d0"), ("1000100010001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oam", "g", "g3"), ("0001000000100000010000001000000100000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oam", "h", "h0"), ("1000000010000000100000001000000010000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oam", "i", "i1"), ("0100000000100000000100000000100000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oam", "j", "j0"), ("1000000000100000000010000000001000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("oam", "k", "k8"), ("0000000010000000000100000000001000000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("oam", "m", "m13"), ("0000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_5() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("g75", cols0);
		//To be indexed: [b, d, g, h, n, o, q, s], to be queried: [b0, d0, g5, h5, n6, o9, q5, s19].
		String [][] records_g75 = new String[40][cols0.length];
		for(int i=0;i<40;i++)
		{
			records_g75[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g75[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g75", records_g75[i]);
		}
		DBApp.createBitMapIndex("g75","b");
		DBApp.createBitMapIndex("g75","d");
		DBApp.createBitMapIndex("g75","g");
		DBApp.createBitMapIndex("g75","h");
		DBApp.createBitMapIndex("g75","n");
		DBApp.createBitMapIndex("g75","o");
		DBApp.createBitMapIndex("g75","q");
		DBApp.createBitMapIndex("g75","s");
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
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9, p7, q5, r3, s1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g75", "b", "b0"), ("1010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g75", "d", "d0"), ("1000100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g75", "g", "g5"), ("0000010000001000000100000010000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g75", "h", "h5"), ("0000010000000100000001000000010000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g75", "n", "n6"), ("0000001000000000000010000000000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g75", "o", "o9"), ("0000000001000000000000001000000000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g75", "q", "q5"), ("0000010000000000000000100000000000000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("g75", "s", "s19"), ("0000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("z3o", cols1);
		//To be indexed: [b, c, d, e, h], to be queried: [b0, c0, d0, e2, h8].
		String [][] records_z3o = new String[5][cols1.length];
		for(int i=0;i<5;i++)
		{
			records_z3o[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_z3o[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("z3o", records_z3o[i]);
		}
		DBApp.createBitMapIndex("z3o","b");
		DBApp.createBitMapIndex("z3o","c");
		DBApp.createBitMapIndex("z3o","d");
		DBApp.createBitMapIndex("z3o","e");
		DBApp.createBitMapIndex("z3o","h");
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		//last 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z3o", "b", "b0"), ("10101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z3o", "c", "c0"), ("10010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z3o", "d", "d0"), ("10001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z3o", "e", "e2"), ("00100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("z3o", "h", "h8"), ("00000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("nm4", cols2);
		//To be indexed: [f, g, h, k, l, m, n, q], to be queried: [f1, g5, h6, k2, l9, m3, n6, q17].
		String [][] records_nm4 = new String[27][cols2.length];
		for(int i=0;i<27;i++)
		{
			records_nm4[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_nm4[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("nm4", records_nm4[i]);
		}
		DBApp.createBitMapIndex("nm4","f");
		DBApp.createBitMapIndex("nm4","g");
		DBApp.createBitMapIndex("nm4","h");
		DBApp.createBitMapIndex("nm4","k");
		DBApp.createBitMapIndex("nm4","l");
		DBApp.createBitMapIndex("nm4","m");
		DBApp.createBitMapIndex("nm4","n");
		DBApp.createBitMapIndex("nm4","q");
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
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11, p10, q9]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nm4", "f", "f1"), ("010000010000010000010000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nm4", "g", "g5"), ("000001000000100000010000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nm4", "h", "h6"), ("000000100000001000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nm4", "k", "k2"), ("001000000000010000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nm4", "l", "l9"), ("000000000100000000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nm4", "m", "m3"), ("000100000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nm4", "n", "n6"), ("000000100000000000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("nm4", "q", "q17"), ("000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("e52qi", cols3);
		//To be indexed: [b, c, e, f, g, h, l, m, n, p], to be queried: [b0, c1, e0, f1, g2, h3, l9, m6, n4, p16].
		String [][] records_e52qi = new String[46][cols3.length];
		for(int i=0;i<46;i++)
		{
			records_e52qi[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_e52qi[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("e52qi", records_e52qi[i]);
		}
		DBApp.createBitMapIndex("e52qi","b");
		DBApp.createBitMapIndex("e52qi","c");
		DBApp.createBitMapIndex("e52qi","e");
		DBApp.createBitMapIndex("e52qi","f");
		DBApp.createBitMapIndex("e52qi","g");
		DBApp.createBitMapIndex("e52qi","h");
		DBApp.createBitMapIndex("e52qi","l");
		DBApp.createBitMapIndex("e52qi","m");
		DBApp.createBitMapIndex("e52qi","n");
		DBApp.createBitMapIndex("e52qi","p");
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
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2, o14, p12]
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0, j5, k1, l9, m6, n3, o0, p13]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e52qi", "b", "b0"), ("1010101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e52qi", "c", "c1"), ("0100100100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e52qi", "e", "e0"), ("1000010000100001000010000100001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e52qi", "f", "f1"), ("0100000100000100000100000100000100000100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e52qi", "g", "g2"), ("0010000001000000100000010000001000000100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e52qi", "h", "h3"), ("0001000000010000000100000001000000010000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e52qi", "l", "l9"), ("0000000001000000000001000000000001000000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e52qi", "m", "m6"), ("0000001000000000000100000000000010000000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e52qi", "n", "n4"), ("0000100000000000001000000000000010000000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("e52qi", "p", "p16"), ("0000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_6() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("m2", cols0);
		//To be indexed: [c, d, f, j, n, p], to be queried: [c1, d0, f0, j7, n8, p16].
		String [][] records_m2 = new String[24][cols0.length];
		for(int i=0;i<24;i++)
		{
			records_m2[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_m2[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("m2", records_m2[i]);
		}
		DBApp.createBitMapIndex("m2","c");
		DBApp.createBitMapIndex("m2","d");
		DBApp.createBitMapIndex("m2","f");
		DBApp.createBitMapIndex("m2","j");
		DBApp.createBitMapIndex("m2","n");
		DBApp.createBitMapIndex("m2","p");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m2", "c", "c1"), ("010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m2", "d", "d0"), ("100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m2", "f", "f0"), ("100000100000100000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m2", "j", "j7"), ("000000010000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m2", "n", "n8"), ("000000001000000000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("m2", "p", "p16"), ("000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("atow", cols1);
		//To be indexed: [c, f, g], to be queried: [c0, f1, g7].
		String [][] records_atow = new String[13][cols1.length];
		for(int i=0;i<13;i++)
		{
			records_atow[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_atow[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("atow", records_atow[i]);
		}
		DBApp.createBitMapIndex("atow","c");
		DBApp.createBitMapIndex("atow","f");
		DBApp.createBitMapIndex("atow","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("atow", "c", "c0"), ("1001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("atow", "f", "f1"), ("0100000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("atow", "g", "g7"), ("0000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("b57", cols2);
		//To be indexed: [b, h, l], to be queried: [b0, h0, l12].
		String [][] records_b57 = new String[47][cols2.length];
		for(int i=0;i<47;i++)
		{
			records_b57[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_b57[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("b57", records_b57[i]);
		}
		DBApp.createBitMapIndex("b57","b");
		DBApp.createBitMapIndex("b57","h");
		DBApp.createBitMapIndex("b57","l");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b57", "b", "b0"), ("10101010101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b57", "h", "h0"), ("10000000100000001000000010000000100000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("b57", "l", "l12"), ("00000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("tc9g", cols3);
		//To be indexed: [b, e, f, g, h, i, l, m, n], to be queried: [b0, e1, f0, g1, h4, i5, l0, m3, n14].
		String [][] records_tc9g = new String[13][cols3.length];
		for(int i=0;i<13;i++)
		{
			records_tc9g[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_tc9g[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("tc9g", records_tc9g[i]);
		}
		DBApp.createBitMapIndex("tc9g","b");
		DBApp.createBitMapIndex("tc9g","e");
		DBApp.createBitMapIndex("tc9g","f");
		DBApp.createBitMapIndex("tc9g","g");
		DBApp.createBitMapIndex("tc9g","h");
		DBApp.createBitMapIndex("tc9g","i");
		DBApp.createBitMapIndex("tc9g","l");
		DBApp.createBitMapIndex("tc9g","m");
		DBApp.createBitMapIndex("tc9g","n");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tc9g", "b", "b0"), ("1010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tc9g", "e", "e1"), ("0100001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tc9g", "f", "f0"), ("1000001000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tc9g", "g", "g1"), ("0100000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tc9g", "h", "h4"), ("0000100000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tc9g", "i", "i5"), ("0000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tc9g", "l", "l0"), ("1000000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tc9g", "m", "m3"), ("0001000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("tc9g", "n", "n14"), ("0000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("fm6i", cols4);
		//To be indexed: [b, c, d, f, g], to be queried: [b0, c1, d0, f2, g7].
		String [][] records_fm6i = new String[32][cols4.length];
		for(int i=0;i<32;i++)
		{
			records_fm6i[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_fm6i[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("fm6i", records_fm6i[i]);
		}
		DBApp.createBitMapIndex("fm6i","b");
		DBApp.createBitMapIndex("fm6i","c");
		DBApp.createBitMapIndex("fm6i","d");
		DBApp.createBitMapIndex("fm6i","f");
		DBApp.createBitMapIndex("fm6i","g");
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
		//record31: [a31, b1, c1, d3, e1, f1, g3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fm6i", "b", "b0"), ("10101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fm6i", "c", "c1"), ("01001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fm6i", "d", "d0"), ("10001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fm6i", "f", "f2"), ("00100000100000100000100000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("fm6i", "g", "g7"), ("00000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_7() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("tj5", cols0);
		//To be indexed: [b, d, g, h, j], to be queried: [b0, d0, g2, h0, j10].
		String [][] records_tj5 = new String[18][cols0.length];
		for(int i=0;i<18;i++)
		{
			records_tj5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_tj5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("tj5", records_tj5[i]);
		}
		DBApp.createBitMapIndex("tj5","b");
		DBApp.createBitMapIndex("tj5","d");
		DBApp.createBitMapIndex("tj5","g");
		DBApp.createBitMapIndex("tj5","h");
		DBApp.createBitMapIndex("tj5","j");
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
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tj5", "b", "b0"), ("101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tj5", "d", "d0"), ("100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tj5", "g", "g2"), ("001000000100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("tj5", "h", "h0"), ("100000001000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("tj5", "j", "j10"), ("000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("h9", cols1);
		//To be indexed: [b, e, f, h, k, l, m, n], to be queried: [b0, e2, f2, h0, k6, l10, m9, n14].
		String [][] records_h9 = new String[6][cols1.length];
		for(int i=0;i<6;i++)
		{
			records_h9[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_h9[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("h9", records_h9[i]);
		}
		DBApp.createBitMapIndex("h9","b");
		DBApp.createBitMapIndex("h9","e");
		DBApp.createBitMapIndex("h9","f");
		DBApp.createBitMapIndex("h9","h");
		DBApp.createBitMapIndex("h9","k");
		DBApp.createBitMapIndex("h9","l");
		DBApp.createBitMapIndex("h9","m");
		DBApp.createBitMapIndex("h9","n");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h9", "b", "b0"), ("101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h9", "e", "e2"), ("001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h9", "f", "f2"), ("001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h9", "h", "h0"), ("100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h9", "k", "k6"), ("000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h9", "l", "l10"), ("000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h9", "m", "m9"), ("000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("h9", "n", "n14"), ("000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("ho64", cols2);
		//To be indexed: [b, c, d, e, g, i, l, m, q, r], to be queried: [b0, c1, d1, e2, g3, i0, l9, m8, q12, r18].
		String [][] records_ho64 = new String[38][cols2.length];
		for(int i=0;i<38;i++)
		{
			records_ho64[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_ho64[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("ho64", records_ho64[i]);
		}
		DBApp.createBitMapIndex("ho64","b");
		DBApp.createBitMapIndex("ho64","c");
		DBApp.createBitMapIndex("ho64","d");
		DBApp.createBitMapIndex("ho64","e");
		DBApp.createBitMapIndex("ho64","g");
		DBApp.createBitMapIndex("ho64","i");
		DBApp.createBitMapIndex("ho64","l");
		DBApp.createBitMapIndex("ho64","m");
		DBApp.createBitMapIndex("ho64","q");
		DBApp.createBitMapIndex("ho64","r");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ho64", "b", "b0"), ("10101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ho64", "c", "c1"), ("01001001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ho64", "d", "d1"), ("01000100010001000100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ho64", "e", "e2"), ("00100001000010000100001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ho64", "g", "g3"), ("00010000001000000100000010000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ho64", "i", "i0"), ("10000000010000000010000000010000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ho64", "l", "l9"), ("00000000010000000000010000000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ho64", "m", "m8"), ("00000000100000000000010000000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ho64", "q", "q12"), ("00000000000010000000000000000100000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ho64", "r", "r18"), ("00000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("hy3w", cols3);
		//To be indexed: [b, j, k], to be queried: [b0, j0, k11].
		String [][] records_hy3w = new String[35][cols3.length];
		for(int i=0;i<35;i++)
		{
			records_hy3w[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_hy3w[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("hy3w", records_hy3w[i]);
		}
		DBApp.createBitMapIndex("hy3w","b");
		DBApp.createBitMapIndex("hy3w","j");
		DBApp.createBitMapIndex("hy3w","k");
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
		//record33: [a33, b1, c0, d1, e3, f3, g5, h1, i6, j3, k0]
		//record34: [a34, b0, c1, d2, e4, f4, g6, h2, i7, j4, k1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hy3w", "b", "b0"), ("10101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hy3w", "j", "j0"), ("10000000001000000000100000000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("hy3w", "k", "k11"), ("00000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_8() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("yo13", cols0);
		//To be indexed: [c, i, j, l, m, q, s], to be queried: [c1, i1, j0, l4, m1, q12, s19].
		String [][] records_yo13 = new String[6][cols0.length];
		for(int i=0;i<6;i++)
		{
			records_yo13[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_yo13[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("yo13", records_yo13[i]);
		}
		DBApp.createBitMapIndex("yo13","c");
		DBApp.createBitMapIndex("yo13","i");
		DBApp.createBitMapIndex("yo13","j");
		DBApp.createBitMapIndex("yo13","l");
		DBApp.createBitMapIndex("yo13","m");
		DBApp.createBitMapIndex("yo13","q");
		DBApp.createBitMapIndex("yo13","s");
		//first 6 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5, s5]
		//last 6 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0, r0, s0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3, r3, s3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4, r4, s4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5, r5, s5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yo13", "c", "c1"), ("010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yo13", "i", "i1"), ("010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yo13", "j", "j0"), ("100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yo13", "l", "l4"), ("000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yo13", "m", "m1"), ("010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yo13", "q", "q12"), ("000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("yo13", "s", "s19"), ("000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_9() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("k61", cols0);
		//To be indexed: [b, c, f, g, h, i], to be queried: [b0, c1, f4, g4, h0, i9].
		String [][] records_k61 = new String[10][cols0.length];
		for(int i=0;i<10;i++)
		{
			records_k61[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_k61[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("k61", records_k61[i]);
		}
		DBApp.createBitMapIndex("k61","b");
		DBApp.createBitMapIndex("k61","c");
		DBApp.createBitMapIndex("k61","f");
		DBApp.createBitMapIndex("k61","g");
		DBApp.createBitMapIndex("k61","h");
		DBApp.createBitMapIndex("k61","i");
		//first 10 records:
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
		//last 10 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k61", "b", "b0"), ("1010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k61", "c", "c1"), ("0100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k61", "f", "f4"), ("0000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k61", "g", "g4"), ("0000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k61", "h", "h0"), ("1000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("k61", "i", "i9"), ("0000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("zb09", cols1);
		//To be indexed: [f, i, j, m], to be queried: [f0, i1, j4, m13].
		String [][] records_zb09 = new String[49][cols1.length];
		for(int i=0;i<49;i++)
		{
			records_zb09[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_zb09[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("zb09", records_zb09[i]);
		}
		DBApp.createBitMapIndex("zb09","f");
		DBApp.createBitMapIndex("zb09","i");
		DBApp.createBitMapIndex("zb09","j");
		DBApp.createBitMapIndex("zb09","m");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zb09", "f", "f0"), ("1000001000001000001000001000001000001000001000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zb09", "i", "i1"), ("0100000000100000000100000000100000000100000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("zb09", "j", "j4"), ("0000100000000010000000001000000000100000000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("zb09", "m", "m13"), ("0000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("jr0", cols2);
		//To be indexed: [e, g, i, j, l], to be queried: [e2, g0, i7, j0, l12].
		String [][] records_jr0 = new String[38][cols2.length];
		for(int i=0;i<38;i++)
		{
			records_jr0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_jr0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("jr0", records_jr0[i]);
		}
		DBApp.createBitMapIndex("jr0","e");
		DBApp.createBitMapIndex("jr0","g");
		DBApp.createBitMapIndex("jr0","i");
		DBApp.createBitMapIndex("jr0","j");
		DBApp.createBitMapIndex("jr0","l");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jr0", "e", "e2"), ("00100001000010000100001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jr0", "g", "g0"), ("10000001000000100000010000001000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jr0", "i", "i7"), ("00000001000000001000000001000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jr0", "j", "j0"), ("10000000001000000000100000000010000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("jr0", "l", "l12"), ("00000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("svr09", cols3);
		//To be indexed: [c, g, o], to be queried: [c1, g4, o15].
		String [][] records_svr09 = new String[45][cols3.length];
		for(int i=0;i<45;i++)
		{
			records_svr09[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_svr09[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("svr09", records_svr09[i]);
		}
		DBApp.createBitMapIndex("svr09","c");
		DBApp.createBitMapIndex("svr09","g");
		DBApp.createBitMapIndex("svr09","o");
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
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0, o12]
		//record43: [a43, b1, c1, d3, e3, f1, g1, h3, i7, j3, k10, l7, m4, n1, o13]
		//record44: [a44, b0, c2, d0, e4, f2, g2, h4, i8, j4, k0, l8, m5, n2, o14]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("svr09", "c", "c1"), ("010010010010010010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("svr09", "g", "g4"), ("000010000001000000100000010000001000000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("svr09", "o", "o15"), ("000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("mo4", cols4);
		//To be indexed: [c, f, g, h, k, m, n, q, r, s, t], to be queried: [c1, f2, g4, h2, k5, m2, n9, q14, r5, s1, t20].
		String [][] records_mo4 = new String[46][cols4.length];
		for(int i=0;i<46;i++)
		{
			records_mo4[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_mo4[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("mo4", records_mo4[i]);
		}
		DBApp.createBitMapIndex("mo4","c");
		DBApp.createBitMapIndex("mo4","f");
		DBApp.createBitMapIndex("mo4","g");
		DBApp.createBitMapIndex("mo4","h");
		DBApp.createBitMapIndex("mo4","k");
		DBApp.createBitMapIndex("mo4","m");
		DBApp.createBitMapIndex("mo4","n");
		DBApp.createBitMapIndex("mo4","q");
		DBApp.createBitMapIndex("mo4","r");
		DBApp.createBitMapIndex("mo4","s");
		DBApp.createBitMapIndex("mo4","t");
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
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1, p15, q14, r13, s12, t11]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mo4", "c", "c1"), ("0100100100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mo4", "f", "f2"), ("0010000010000010000010000010000010000010000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mo4", "g", "g4"), ("0000100000010000001000000100000010000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mo4", "h", "h2"), ("0010000000100000001000000010000000100000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mo4", "k", "k5"), ("0000010000000000100000000001000000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mo4", "m", "m2"), ("0010000000000001000000000000100000000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mo4", "n", "n9"), ("0000000001000000000000010000000000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mo4", "q", "q14"), ("0000000000000010000000000000000100000000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mo4", "r", "r5"), ("0000010000000000000000010000000000000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mo4", "s", "s1"), ("0100000000000000000010000000000000000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("mo4", "t", "t20"), ("0000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_10() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 2;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("yma", cols0);
		//To be indexed: [d, g, h, i], to be queried: [d0, g0, h4, i9].
		String [][] records_yma = new String[45][cols0.length];
		for(int i=0;i<45;i++)
		{
			records_yma[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_yma[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("yma", records_yma[i]);
		}
		DBApp.createBitMapIndex("yma","d");
		DBApp.createBitMapIndex("yma","g");
		DBApp.createBitMapIndex("yma","h");
		DBApp.createBitMapIndex("yma","i");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yma", "d", "d0"), ("100010001000100010001000100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yma", "g", "g0"), ("100000010000001000000100000010000001000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("yma", "h", "h4"), ("000010000000100000001000000010000000100000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("yma", "i", "i9"), ("000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h"};
		DBApp.createTable("s4n", cols1);
		//To be indexed: [c, e, f, g, h], to be queried: [c0, e1, f4, g5, h8].
		String [][] records_s4n = new String[4][cols1.length];
		for(int i=0;i<4;i++)
		{
			records_s4n[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_s4n[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("s4n", records_s4n[i]);
		}
		DBApp.createBitMapIndex("s4n","c");
		DBApp.createBitMapIndex("s4n","e");
		DBApp.createBitMapIndex("s4n","f");
		DBApp.createBitMapIndex("s4n","g");
		DBApp.createBitMapIndex("s4n","h");
		//first 4 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		//last 4 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s4n", "c", "c0"), ("1001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s4n", "e", "e1"), ("0100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s4n", "f", "f4"), ("0000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("s4n", "g", "g5"), ("0000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("s4n", "h", "h8"), ("0000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("nk", cols2);
		//To be indexed: [b, c, g], to be queried: [b0, c1, g7].
		String [][] records_nk = new String[19][cols2.length];
		for(int i=0;i<19;i++)
		{
			records_nk[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_nk[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("nk", records_nk[i]);
		}
		DBApp.createBitMapIndex("nk","b");
		DBApp.createBitMapIndex("nk","c");
		DBApp.createBitMapIndex("nk","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nk", "b", "b0"), ("1010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("nk", "c", "c1"), ("0100100100100100100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("nk", "g", "g7"), ("0000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("y46", cols3);
		//To be indexed: [e, f, g, h, i, j, l, o, q], to be queried: [e2, f2, g1, h0, i1, j1, l3, o8, q17].
		String [][] records_y46 = new String[20][cols3.length];
		for(int i=0;i<20;i++)
		{
			records_y46[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_y46[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("y46", records_y46[i]);
		}
		DBApp.createBitMapIndex("y46","e");
		DBApp.createBitMapIndex("y46","f");
		DBApp.createBitMapIndex("y46","g");
		DBApp.createBitMapIndex("y46","h");
		DBApp.createBitMapIndex("y46","i");
		DBApp.createBitMapIndex("y46","j");
		DBApp.createBitMapIndex("y46","l");
		DBApp.createBitMapIndex("y46","o");
		DBApp.createBitMapIndex("y46","q");
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
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3, p2, q1]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4, p3, q2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y46", "e", "e2"), ("00100001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y46", "f", "f2"), ("00100000100000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y46", "g", "g1"), ("01000000100000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y46", "h", "h0"), ("10000000100000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y46", "i", "i1"), ("01000000001000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y46", "j", "j1"), ("01000000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y46", "l", "l3"), ("00010000000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y46", "o", "o8"), ("00000000100000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("y46", "q", "q17"), ("00000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("rtl13", cols4);
		//To be indexed: [h, j, l, m], to be queried: [h0, j4, l0, m13].
		String [][] records_rtl13 = new String[48][cols4.length];
		for(int i=0;i<48;i++)
		{
			records_rtl13[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_rtl13[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("rtl13", records_rtl13[i]);
		}
		DBApp.createBitMapIndex("rtl13","h");
		DBApp.createBitMapIndex("rtl13","j");
		DBApp.createBitMapIndex("rtl13","l");
		DBApp.createBitMapIndex("rtl13","m");
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
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1, j6, k2, l10, m7]
		//record47: [a47, b1, c2, d3, e2, f5, g5, h7, i2, j7, k3, l11, m8]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("rtl13", "h", "h0"), ("100000001000000010000000100000001000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("rtl13", "j", "j4"), ("000010000000001000000000100000000010000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("rtl13", "l", "l0"), ("100000000000100000000000100000000000100000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("rtl13", "m", "m13"), ("000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_11() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s"};
		DBApp.createTable("hf5", cols0);
		//To be indexed: [b, c, d, e, f, g, k, m, n, o, p, q, r, s], to be queried: [b0, c1, d1, e0, f3, g5, k8, m2, n10, o1, p1, q7, r0, s19].
		String [][] records_hf5 = new String[43][cols0.length];
		for(int i=0;i<43;i++)
		{
			records_hf5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_hf5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("hf5", records_hf5[i]);
		}
		DBApp.createBitMapIndex("hf5","b");
		DBApp.createBitMapIndex("hf5","c");
		DBApp.createBitMapIndex("hf5","d");
		DBApp.createBitMapIndex("hf5","e");
		DBApp.createBitMapIndex("hf5","f");
		DBApp.createBitMapIndex("hf5","g");
		DBApp.createBitMapIndex("hf5","k");
		DBApp.createBitMapIndex("hf5","m");
		DBApp.createBitMapIndex("hf5","n");
		DBApp.createBitMapIndex("hf5","o");
		DBApp.createBitMapIndex("hf5","p");
		DBApp.createBitMapIndex("hf5","q");
		DBApp.createBitMapIndex("hf5","r");
		DBApp.createBitMapIndex("hf5","s");
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
		//record39: [a39, b1, c0, d3, e4, f3, g4, h7, i3, j9, k6, l3, m0, n11, o9, p7, q5, r3, s1]
		//record40: [a40, b0, c1, d0, e0, f4, g5, h0, i4, j0, k7, l4, m1, n12, o10, p8, q6, r4, s2]
		//record41: [a41, b1, c2, d1, e1, f5, g6, h1, i5, j1, k8, l5, m2, n13, o11, p9, q7, r5, s3]
		//record42: [a42, b0, c0, d2, e2, f0, g0, h2, i6, j2, k9, l6, m3, n0, o12, p10, q8, r6, s4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hf5", "b", "b0"), ("1010101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hf5", "c", "c1"), ("0100100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hf5", "d", "d1"), ("0100010001000100010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hf5", "e", "e0"), ("1000010000100001000010000100001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hf5", "f", "f3"), ("0001000001000001000001000001000001000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hf5", "g", "g5"), ("0000010000001000000100000010000001000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hf5", "k", "k8"), ("0000000010000000000100000000001000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hf5", "m", "m2"), ("0010000000000001000000000000100000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hf5", "n", "n10"), ("0000000000100000000000001000000000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hf5", "o", "o1"), ("0100000000000000100000000000000100000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hf5", "p", "p1"), ("0100000000000000010000000000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hf5", "q", "q7"), ("0000000100000000000000001000000000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hf5", "r", "r0"), ("1000000000000000001000000000000000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("hf5", "s", "s19"), ("0000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("v5", cols1);
		//To be indexed: [b, c, g, k, l, n, p], to be queried: [b0, c1, g0, k7, l6, n1, p16].
		String [][] records_v5 = new String[31][cols1.length];
		for(int i=0;i<31;i++)
		{
			records_v5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_v5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("v5", records_v5[i]);
		}
		DBApp.createBitMapIndex("v5","b");
		DBApp.createBitMapIndex("v5","c");
		DBApp.createBitMapIndex("v5","g");
		DBApp.createBitMapIndex("v5","k");
		DBApp.createBitMapIndex("v5","l");
		DBApp.createBitMapIndex("v5","n");
		DBApp.createBitMapIndex("v5","p");
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
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12, p11]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14, p13]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v5", "b", "b0"), ("1010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v5", "c", "c1"), ("0100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v5", "g", "g0"), ("1000000100000010000001000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v5", "k", "k7"), ("0000000100000000001000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v5", "l", "l6"), ("0000001000000000001000000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v5", "n", "n1"), ("0100000000000001000000000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("v5", "p", "p16"), ("0000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("bhoo", cols2);
		//To be indexed: [c, d, h, i, j, k], to be queried: [c0, d1, h6, i1, j5, k11].
		String [][] records_bhoo = new String[45][cols2.length];
		for(int i=0;i<45;i++)
		{
			records_bhoo[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_bhoo[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("bhoo", records_bhoo[i]);
		}
		DBApp.createBitMapIndex("bhoo","c");
		DBApp.createBitMapIndex("bhoo","d");
		DBApp.createBitMapIndex("bhoo","h");
		DBApp.createBitMapIndex("bhoo","i");
		DBApp.createBitMapIndex("bhoo","j");
		DBApp.createBitMapIndex("bhoo","k");
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
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8]
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bhoo", "c", "c0"), ("100100100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bhoo", "d", "d1"), ("010001000100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bhoo", "h", "h6"), ("000000100000001000000010000000100000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bhoo", "i", "i1"), ("010000000010000000010000000010000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bhoo", "j", "j5"), ("000001000000000100000000010000000001000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("bhoo", "k", "k11"), ("000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("czh", cols3);
		//To be indexed: [b, e, i, j, l, m], to be queried: [b0, e1, i6, j1, l9, m13].
		String [][] records_czh = new String[16][cols3.length];
		for(int i=0;i<16;i++)
		{
			records_czh[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_czh[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("czh", records_czh[i]);
		}
		DBApp.createBitMapIndex("czh","b");
		DBApp.createBitMapIndex("czh","e");
		DBApp.createBitMapIndex("czh","i");
		DBApp.createBitMapIndex("czh","j");
		DBApp.createBitMapIndex("czh","l");
		DBApp.createBitMapIndex("czh","m");
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
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("czh", "b", "b0"), ("1010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("czh", "e", "e1"), ("0100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("czh", "i", "i6"), ("0000001000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("czh", "j", "j1"), ("0100000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("czh", "l", "l9"), ("0000000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("czh", "m", "m13"), ("0000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("h0d", cols4);
		//To be indexed: [c, d, f, g], to be queried: [c1, d1, f4, g7].
		String [][] records_h0d = new String[10][cols4.length];
		for(int i=0;i<10;i++)
		{
			records_h0d[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_h0d[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("h0d", records_h0d[i]);
		}
		DBApp.createBitMapIndex("h0d","c");
		DBApp.createBitMapIndex("h0d","d");
		DBApp.createBitMapIndex("h0d","f");
		DBApp.createBitMapIndex("h0d","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h0d", "c", "c1"), ("0100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h0d", "d", "d1"), ("0100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h0d", "f", "f4"), ("0000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("h0d", "g", "g7"), ("0000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_12() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("n8c", cols0);
		//To be indexed: [c, g], to be queried: [c0, g7].
		String [][] records_n8c = new String[13][cols0.length];
		for(int i=0;i<13;i++)
		{
			records_n8c[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_n8c[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("n8c", records_n8c[i]);
		}
		DBApp.createBitMapIndex("n8c","c");
		DBApp.createBitMapIndex("n8c","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n8c", "c", "c0"), ("1001001001001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("n8c", "g", "g7"), ("0000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("w2ey", cols1);
		//To be indexed: [c, d, f, g, h, k, n, r], to be queried: [c0, d0, f4, g4, h3, k7, n12, r18].
		String [][] records_w2ey = new String[20][cols1.length];
		for(int i=0;i<20;i++)
		{
			records_w2ey[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w2ey[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w2ey", records_w2ey[i]);
		}
		DBApp.createBitMapIndex("w2ey","c");
		DBApp.createBitMapIndex("w2ey","d");
		DBApp.createBitMapIndex("w2ey","f");
		DBApp.createBitMapIndex("w2ey","g");
		DBApp.createBitMapIndex("w2ey","h");
		DBApp.createBitMapIndex("w2ey","k");
		DBApp.createBitMapIndex("w2ey","n");
		DBApp.createBitMapIndex("w2ey","r");
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
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2, n1, o0, p15, q15, r15]
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2, o1, p0, q16, r16]
		//record17: [a17, b1, c2, d1, e2, f5, g3, h1, i8, j7, k6, l5, m4, n3, o2, p1, q0, r17]
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0, j8, k7, l6, m5, n4, o3, p2, q1, r0]
		//record19: [a19, b1, c1, d3, e4, f1, g5, h3, i1, j9, k8, l7, m6, n5, o4, p3, q2, r1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w2ey", "c", "c0"), ("10010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w2ey", "d", "d0"), ("10001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w2ey", "f", "f4"), ("00001000001000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w2ey", "g", "g4"), ("00001000000100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w2ey", "h", "h3"), ("00010000000100000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w2ey", "k", "k7"), ("00000001000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w2ey", "n", "n12"), ("00000000000010000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("w2ey", "r", "r18"), ("00000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("uq0", cols2);
		//To be indexed: [b, c, d, g], to be queried: [b0, c1, d0, g7].
		String [][] records_uq0 = new String[17][cols2.length];
		for(int i=0;i<17;i++)
		{
			records_uq0[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_uq0[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("uq0", records_uq0[i]);
		}
		DBApp.createBitMapIndex("uq0","b");
		DBApp.createBitMapIndex("uq0","c");
		DBApp.createBitMapIndex("uq0","d");
		DBApp.createBitMapIndex("uq0","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uq0", "b", "b0"), ("10101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uq0", "c", "c1"), ("01001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uq0", "d", "d0"), ("10001000100010001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("uq0", "g", "g7"), ("00000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("hr8", cols3);
		//To be indexed: [f, i, k, l, m], to be queried: [f3, i2, k7, l0, m13].
		String [][] records_hr8 = new String[29][cols3.length];
		for(int i=0;i<29;i++)
		{
			records_hr8[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_hr8[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("hr8", records_hr8[i]);
		}
		DBApp.createBitMapIndex("hr8","f");
		DBApp.createBitMapIndex("hr8","i");
		DBApp.createBitMapIndex("hr8","k");
		DBApp.createBitMapIndex("hr8","l");
		DBApp.createBitMapIndex("hr8","m");
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
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hr8", "f", "f3"), ("00010000010000010000010000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hr8", "i", "i2"), ("00100000000100000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hr8", "k", "k7"), ("00000001000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("hr8", "l", "l0"), ("10000000000010000000000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("hr8", "m", "m13"), ("00000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("m935", cols4);
		//To be indexed: [b, e, f, k], to be queried: [b0, e1, f3, k11].
		String [][] records_m935 = new String[31][cols4.length];
		for(int i=0;i<31;i++)
		{
			records_m935[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_m935[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("m935", records_m935[i]);
		}
		DBApp.createBitMapIndex("m935","b");
		DBApp.createBitMapIndex("m935","e");
		DBApp.createBitMapIndex("m935","f");
		DBApp.createBitMapIndex("m935","k");
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
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m935", "b", "b0"), ("1010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m935", "e", "e1"), ("0100001000010000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m935", "f", "f3"), ("0001000001000001000001000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("m935", "k", "k11"), ("0000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_13() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("v4a", cols0);
		//To be indexed: [b, c, d, e, f, j, l, m, n, o, q], to be queried: [b0, c1, d1, e2, f1, j7, l5, m9, n5, o0, q17].
		String [][] records_v4a = new String[29][cols0.length];
		for(int i=0;i<29;i++)
		{
			records_v4a[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_v4a[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("v4a", records_v4a[i]);
		}
		DBApp.createBitMapIndex("v4a","b");
		DBApp.createBitMapIndex("v4a","c");
		DBApp.createBitMapIndex("v4a","d");
		DBApp.createBitMapIndex("v4a","e");
		DBApp.createBitMapIndex("v4a","f");
		DBApp.createBitMapIndex("v4a","j");
		DBApp.createBitMapIndex("v4a","l");
		DBApp.createBitMapIndex("v4a","m");
		DBApp.createBitMapIndex("v4a","n");
		DBApp.createBitMapIndex("v4a","o");
		DBApp.createBitMapIndex("v4a","q");
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
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12, q11]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v4a", "b", "b0"), ("10101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v4a", "c", "c1"), ("01001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v4a", "d", "d1"), ("01000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v4a", "e", "e2"), ("00100001000010000100001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v4a", "f", "f1"), ("01000001000001000001000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v4a", "j", "j7"), ("00000001000000000100000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v4a", "l", "l5"), ("00000100000000000100000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v4a", "m", "m9"), ("00000000010000000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v4a", "n", "n5"), ("00000100000000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("v4a", "o", "o0"), ("10000000000000010000000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("v4a", "q", "q17"), ("00000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("vcj", cols1);
		//To be indexed: [f, g, h, i, j, n, o], to be queried: [f4, g2, h2, i3, j6, n10, o15].
		String [][] records_vcj = new String[41][cols1.length];
		for(int i=0;i<41;i++)
		{
			records_vcj[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_vcj[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("vcj", records_vcj[i]);
		}
		DBApp.createBitMapIndex("vcj","f");
		DBApp.createBitMapIndex("vcj","g");
		DBApp.createBitMapIndex("vcj","h");
		DBApp.createBitMapIndex("vcj","i");
		DBApp.createBitMapIndex("vcj","j");
		DBApp.createBitMapIndex("vcj","n");
		DBApp.createBitMapIndex("vcj","o");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vcj", "f", "f4"), ("00001000001000001000001000001000001000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vcj", "g", "g2"), ("00100000010000001000000100000010000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vcj", "h", "h2"), ("00100000001000000010000000100000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vcj", "i", "i3"), ("00010000000010000000010000000010000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vcj", "j", "j6"), ("00000010000000001000000000100000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vcj", "n", "n10"), ("00000000001000000000000010000000000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("vcj", "o", "o15"), ("00000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("wzf", cols2);
		//To be indexed: [c, d, e, g], to be queried: [c0, d0, e1, g7].
		String [][] records_wzf = new String[31][cols2.length];
		for(int i=0;i<31;i++)
		{
			records_wzf[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_wzf[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("wzf", records_wzf[i]);
		}
		DBApp.createBitMapIndex("wzf","c");
		DBApp.createBitMapIndex("wzf","d");
		DBApp.createBitMapIndex("wzf","e");
		DBApp.createBitMapIndex("wzf","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wzf", "c", "c0"), ("1001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wzf", "d", "d0"), ("1000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("wzf", "e", "e1"), ("0100001000010000100001000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("wzf", "g", "g7"), ("0000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_14() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("p6", cols0);
		//To be indexed: [c, d, e, f, h, i, m, n, q, s, t], to be queried: [c1, d2, e3, f3, h1, i4, m5, n6, q11, s3, t20].
		String [][] records_p6 = new String[15][cols0.length];
		for(int i=0;i<15;i++)
		{
			records_p6[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_p6[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("p6", records_p6[i]);
		}
		DBApp.createBitMapIndex("p6","c");
		DBApp.createBitMapIndex("p6","d");
		DBApp.createBitMapIndex("p6","e");
		DBApp.createBitMapIndex("p6","f");
		DBApp.createBitMapIndex("p6","h");
		DBApp.createBitMapIndex("p6","i");
		DBApp.createBitMapIndex("p6","m");
		DBApp.createBitMapIndex("p6","n");
		DBApp.createBitMapIndex("p6","q");
		DBApp.createBitMapIndex("p6","s");
		DBApp.createBitMapIndex("p6","t");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6", "c", "c1"), ("010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6", "d", "d2"), ("001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6", "e", "e3"), ("000100001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6", "f", "f3"), ("000100000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6", "h", "h1"), ("010000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6", "i", "i4"), ("000010000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6", "m", "m5"), ("000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6", "n", "n6"), ("000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6", "q", "q11"), ("000000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("p6", "s", "s3"), ("000100000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("p6", "t", "t20"), ("000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("t92", cols1);
		//To be indexed: [b, c, d, f, g, j, k, l, m, n, o, p, q], to be queried: [b0, c0, d0, f3, g4, j1, k7, l3, m5, n2, o12, p0, q17].
		String [][] records_t92 = new String[14][cols1.length];
		for(int i=0;i<14;i++)
		{
			records_t92[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_t92[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("t92", records_t92[i]);
		}
		DBApp.createBitMapIndex("t92","b");
		DBApp.createBitMapIndex("t92","c");
		DBApp.createBitMapIndex("t92","d");
		DBApp.createBitMapIndex("t92","f");
		DBApp.createBitMapIndex("t92","g");
		DBApp.createBitMapIndex("t92","j");
		DBApp.createBitMapIndex("t92","k");
		DBApp.createBitMapIndex("t92","l");
		DBApp.createBitMapIndex("t92","m");
		DBApp.createBitMapIndex("t92","n");
		DBApp.createBitMapIndex("t92","o");
		DBApp.createBitMapIndex("t92","p");
		DBApp.createBitMapIndex("t92","q");
		//first 14 records:
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
		//last 14 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t92", "b", "b0"), ("10101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t92", "c", "c0"), ("10010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t92", "d", "d0"), ("10001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t92", "f", "f3"), ("00010000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t92", "g", "g4"), ("00001000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t92", "j", "j1"), ("01000000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t92", "k", "k7"), ("00000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t92", "l", "l3"), ("00010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t92", "m", "m5"), ("00000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t92", "n", "n2"), ("00100000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t92", "o", "o12"), ("00000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t92", "p", "p0"), ("10000000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("t92", "q", "q17"), ("00000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_15() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("t2jr", cols0);
		//To be indexed: [c, e, g], to be queried: [c0, e0, g7].
		String [][] records_t2jr = new String[46][cols0.length];
		for(int i=0;i<46;i++)
		{
			records_t2jr[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_t2jr[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("t2jr", records_t2jr[i]);
		}
		DBApp.createBitMapIndex("t2jr","c");
		DBApp.createBitMapIndex("t2jr","e");
		DBApp.createBitMapIndex("t2jr","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t2jr", "c", "c0"), ("1001001001001001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("t2jr", "e", "e0"), ("1000010000100001000010000100001000010000100001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("t2jr", "g", "g7"), ("0000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("w1", cols1);
		//To be indexed: [d, e, i, k], to be queried: [d0, e0, i7, k11].
		String [][] records_w1 = new String[23][cols1.length];
		for(int i=0;i<23;i++)
		{
			records_w1[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_w1[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("w1", records_w1[i]);
		}
		DBApp.createBitMapIndex("w1","d");
		DBApp.createBitMapIndex("w1","e");
		DBApp.createBitMapIndex("w1","i");
		DBApp.createBitMapIndex("w1","k");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w1", "d", "d0"), ("10001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w1", "e", "e0"), ("10000100001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("w1", "i", "i7"), ("00000001000000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("w1", "k", "k11"), ("00000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("z37", cols2);
		//To be indexed: [b, d, g, h, i], to be queried: [b0, d2, g3, h1, i9].
		String [][] records_z37 = new String[50][cols2.length];
		for(int i=0;i<50;i++)
		{
			records_z37[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_z37[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("z37", records_z37[i]);
		}
		DBApp.createBitMapIndex("z37","b");
		DBApp.createBitMapIndex("z37","d");
		DBApp.createBitMapIndex("z37","g");
		DBApp.createBitMapIndex("z37","h");
		DBApp.createBitMapIndex("z37","i");
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
		//record48: [a48, b0, c0, d0, e3, f0, g6, h0, i3]
		//record49: [a49, b1, c1, d1, e4, f1, g0, h1, i4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z37", "b", "b0"), ("10101010101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z37", "d", "d2"), ("00100010001000100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z37", "g", "g3"), ("00010000001000000100000010000001000000100000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("z37", "h", "h1"), ("01000000010000000100000001000000010000000100000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("z37", "i", "i9"), ("00000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("h08", cols3);
		//To be indexed: [c, f, k, l, m, p], to be queried: [c1, f1, k9, l4, m4, p16].
		String [][] records_h08 = new String[5][cols3.length];
		for(int i=0;i<5;i++)
		{
			records_h08[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_h08[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("h08", records_h08[i]);
		}
		DBApp.createBitMapIndex("h08","c");
		DBApp.createBitMapIndex("h08","f");
		DBApp.createBitMapIndex("h08","k");
		DBApp.createBitMapIndex("h08","l");
		DBApp.createBitMapIndex("h08","m");
		DBApp.createBitMapIndex("h08","p");
		//first 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		//last 5 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h08", "c", "c1"), ("01001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h08", "f", "f1"), ("01000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h08", "k", "k9"), ("00000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h08", "l", "l4"), ("00001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h08", "m", "m4"), ("00001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("h08", "p", "p16"), ("00000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_16() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("e9mk", cols0);
		//To be indexed: [c, d, f, j, m], to be queried: [c1, d1, f3, j8, m13].
		String [][] records_e9mk = new String[10][cols0.length];
		for(int i=0;i<10;i++)
		{
			records_e9mk[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_e9mk[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("e9mk", records_e9mk[i]);
		}
		DBApp.createBitMapIndex("e9mk","c");
		DBApp.createBitMapIndex("e9mk","d");
		DBApp.createBitMapIndex("e9mk","f");
		DBApp.createBitMapIndex("e9mk","j");
		DBApp.createBitMapIndex("e9mk","m");
		//first 10 records:
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
		//last 10 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e9mk", "c", "c1"), ("0100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e9mk", "d", "d1"), ("0100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e9mk", "f", "f3"), ("0001000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e9mk", "j", "j8"), ("0000000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("e9mk", "m", "m13"), ("0000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("af780", cols1);
		//To be indexed: [b, c, e, h, i, k, m, o, p], to be queried: [b0, c1, e3, h6, i3, k4, m2, o9, p16].
		String [][] records_af780 = new String[47][cols1.length];
		for(int i=0;i<47;i++)
		{
			records_af780[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_af780[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("af780", records_af780[i]);
		}
		DBApp.createBitMapIndex("af780","b");
		DBApp.createBitMapIndex("af780","c");
		DBApp.createBitMapIndex("af780","e");
		DBApp.createBitMapIndex("af780","h");
		DBApp.createBitMapIndex("af780","i");
		DBApp.createBitMapIndex("af780","k");
		DBApp.createBitMapIndex("af780","m");
		DBApp.createBitMapIndex("af780","o");
		DBApp.createBitMapIndex("af780","p");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("af780", "b", "b0"), ("10101010101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("af780", "c", "c1"), ("01001001001001001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("af780", "e", "e3"), ("00010000100001000010000100001000010000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("af780", "h", "h6"), ("00000010000000100000001000000010000000100000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("af780", "i", "i3"), ("00010000000010000000010000000010000000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("af780", "k", "k4"), ("00001000000000010000000000100000000001000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("af780", "m", "m2"), ("00100000000000010000000000001000000000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("af780", "o", "o9"), ("00000000010000000000000010000000000000010000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("af780", "p", "p16"), ("00000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("vab2f", cols2);
		//To be indexed: [c, d, g, j, m, n, o], to be queried: [c0, d0, g5, j2, m6, n5, o15].
		String [][] records_vab2f = new String[34][cols2.length];
		for(int i=0;i<34;i++)
		{
			records_vab2f[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_vab2f[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("vab2f", records_vab2f[i]);
		}
		DBApp.createBitMapIndex("vab2f","c");
		DBApp.createBitMapIndex("vab2f","d");
		DBApp.createBitMapIndex("vab2f","g");
		DBApp.createBitMapIndex("vab2f","j");
		DBApp.createBitMapIndex("vab2f","m");
		DBApp.createBitMapIndex("vab2f","n");
		DBApp.createBitMapIndex("vab2f","o");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vab2f", "c", "c0"), ("1001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vab2f", "d", "d0"), ("1000100010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vab2f", "g", "g5"), ("0000010000001000000100000010000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vab2f", "j", "j2"), ("0010000000001000000000100000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vab2f", "m", "m6"), ("0000001000000000000100000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("vab2f", "n", "n5"), ("0000010000000000000100000000000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("vab2f", "o", "o15"), ("0000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("r06", cols3);
		//To be indexed: [d, f, g, i], to be queried: [d1, f3, g0, i9].
		String [][] records_r06 = new String[1][cols3.length];
		for(int i=0;i<1;i++)
		{
			records_r06[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_r06[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("r06", records_r06[i]);
		}
		DBApp.createBitMapIndex("r06","d");
		DBApp.createBitMapIndex("r06","f");
		DBApp.createBitMapIndex("r06","g");
		DBApp.createBitMapIndex("r06","i");
		//first 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		//last 1 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r06", "d", "d1"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r06", "f", "f3"), ("0"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("r06", "g", "g0"), ("1"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("r06", "i", "i9"), ("0"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_17() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("o9o", cols0);
		//To be indexed: [c, d, h, i, k, l, m], to be queried: [c0, d2, h3, i7, k5, l7, m13].
		String [][] records_o9o = new String[34][cols0.length];
		for(int i=0;i<34;i++)
		{
			records_o9o[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_o9o[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("o9o", records_o9o[i]);
		}
		DBApp.createBitMapIndex("o9o","c");
		DBApp.createBitMapIndex("o9o","d");
		DBApp.createBitMapIndex("o9o","h");
		DBApp.createBitMapIndex("o9o","i");
		DBApp.createBitMapIndex("o9o","k");
		DBApp.createBitMapIndex("o9o","l");
		DBApp.createBitMapIndex("o9o","m");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o9o", "c", "c0"), ("1001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o9o", "d", "d2"), ("0010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o9o", "h", "h3"), ("0001000000010000000100000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o9o", "i", "i7"), ("0000000100000000100000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o9o", "k", "k5"), ("0000010000000000100000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o9o", "l", "l7"), ("0000000100000000000100000000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("o9o", "m", "m13"), ("0000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		DBApp.createTable("dqv", cols1);
		//To be indexed: [b, c, e, f, g, h, l, o], to be queried: [b0, c0, e0, f1, g5, h0, l1, o15].
		String [][] records_dqv = new String[23][cols1.length];
		for(int i=0;i<23;i++)
		{
			records_dqv[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_dqv[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("dqv", records_dqv[i]);
		}
		DBApp.createBitMapIndex("dqv","b");
		DBApp.createBitMapIndex("dqv","c");
		DBApp.createBitMapIndex("dqv","e");
		DBApp.createBitMapIndex("dqv","f");
		DBApp.createBitMapIndex("dqv","g");
		DBApp.createBitMapIndex("dqv","h");
		DBApp.createBitMapIndex("dqv","l");
		DBApp.createBitMapIndex("dqv","o");
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
		//record20: [a20, b0, c2, d0, e0, f2, g6, h4, i2, j0, k9, l8, m7, n6, o5]
		//record21: [a21, b1, c0, d1, e1, f3, g0, h5, i3, j1, k10, l9, m8, n7, o6]
		//record22: [a22, b0, c1, d2, e2, f4, g1, h6, i4, j2, k0, l10, m9, n8, o7]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dqv", "b", "b0"), ("10101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dqv", "c", "c0"), ("10010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dqv", "e", "e0"), ("10000100001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dqv", "f", "f1"), ("01000001000001000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dqv", "g", "g5"), ("00000100000010000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dqv", "h", "h0"), ("10000000100000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("dqv", "l", "l1"), ("01000000000001000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("dqv", "o", "o15"), ("00000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_18() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("k5qw5", cols0);
		//To be indexed: [b, d, e, g], to be queried: [b0, d0, e3, g7].
		String [][] records_k5qw5 = new String[6][cols0.length];
		for(int i=0;i<6;i++)
		{
			records_k5qw5[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_k5qw5[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("k5qw5", records_k5qw5[i]);
		}
		DBApp.createBitMapIndex("k5qw5","b");
		DBApp.createBitMapIndex("k5qw5","d");
		DBApp.createBitMapIndex("k5qw5","e");
		DBApp.createBitMapIndex("k5qw5","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k5qw5", "b", "b0"), ("101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k5qw5", "d", "d0"), ("100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("k5qw5", "e", "e3"), ("000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("k5qw5", "g", "g7"), ("000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("rb", cols1);
		//To be indexed: [c, d, e, g], to be queried: [c0, d2, e1, g7].
		String [][] records_rb = new String[9][cols1.length];
		for(int i=0;i<9;i++)
		{
			records_rb[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_rb[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("rb", records_rb[i]);
		}
		DBApp.createBitMapIndex("rb","c");
		DBApp.createBitMapIndex("rb","d");
		DBApp.createBitMapIndex("rb","e");
		DBApp.createBitMapIndex("rb","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("rb", "c", "c0"), ("100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("rb", "d", "d2"), ("001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("rb", "e", "e1"), ("010000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("rb", "g", "g7"), ("000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_19() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ebvp", cols0);
		//To be indexed: [e, f, g], to be queried: [e2, f0, g7].
		String [][] records_ebvp = new String[15][cols0.length];
		for(int i=0;i<15;i++)
		{
			records_ebvp[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ebvp[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ebvp", records_ebvp[i]);
		}
		DBApp.createBitMapIndex("ebvp","e");
		DBApp.createBitMapIndex("ebvp","f");
		DBApp.createBitMapIndex("ebvp","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ebvp", "e", "e2"), ("001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ebvp", "f", "f0"), ("100000100000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ebvp", "g", "g7"), ("000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("pj", cols1);
		//To be indexed: [d, g, h, i], to be queried: [d2, g4, h0, i9].
		String [][] records_pj = new String[46][cols1.length];
		for(int i=0;i<46;i++)
		{
			records_pj[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_pj[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("pj", records_pj[i]);
		}
		DBApp.createBitMapIndex("pj","d");
		DBApp.createBitMapIndex("pj","g");
		DBApp.createBitMapIndex("pj","h");
		DBApp.createBitMapIndex("pj","i");
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
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("pj", "d", "d2"), ("0010001000100010001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("pj", "g", "g4"), ("0000100000010000001000000100000010000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("pj", "h", "h0"), ("1000000010000000100000001000000010000000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("pj", "i", "i9"), ("0000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("fboa", cols2);
		//To be indexed: [b, c, d, f, g], to be queried: [b0, c1, d0, f2, g7].
		String [][] records_fboa = new String[15][cols2.length];
		for(int i=0;i<15;i++)
		{
			records_fboa[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_fboa[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("fboa", records_fboa[i]);
		}
		DBApp.createBitMapIndex("fboa","b");
		DBApp.createBitMapIndex("fboa","c");
		DBApp.createBitMapIndex("fboa","d");
		DBApp.createBitMapIndex("fboa","f");
		DBApp.createBitMapIndex("fboa","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fboa", "b", "b0"), ("101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fboa", "c", "c1"), ("010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fboa", "d", "d0"), ("100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fboa", "f", "f2"), ("001000001000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("fboa", "g", "g7"), ("000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_20() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("g524", cols0);
		//To be indexed: [b, c, d, g], to be queried: [b0, c0, d0, g7].
		String [][] records_g524 = new String[13][cols0.length];
		for(int i=0;i<13;i++)
		{
			records_g524[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g524[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g524", records_g524[i]);
		}
		DBApp.createBitMapIndex("g524","b");
		DBApp.createBitMapIndex("g524","c");
		DBApp.createBitMapIndex("g524","d");
		DBApp.createBitMapIndex("g524","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g524", "b", "b0"), ("1010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g524", "c", "c0"), ("1001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g524", "d", "d0"), ("1000100010001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("g524", "g", "g7"), ("0000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("e0", cols1);
		//To be indexed: [b, e, i], to be queried: [b0, e0, i9].
		String [][] records_e0 = new String[19][cols1.length];
		for(int i=0;i<19;i++)
		{
			records_e0[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_e0[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("e0", records_e0[i]);
		}
		DBApp.createBitMapIndex("e0","b");
		DBApp.createBitMapIndex("e0","e");
		DBApp.createBitMapIndex("e0","i");
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
		//record18: [a18, b0, c0, d2, e3, f0, g4, h2, i0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e0", "b", "b0"), ("1010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e0", "e", "e0"), ("1000010000100001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("e0", "i", "i9"), ("0000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("h1", cols2);
		//To be indexed: [d, e, g], to be queried: [d2, e1, g7].
		String [][] records_h1 = new String[32][cols2.length];
		for(int i=0;i<32;i++)
		{
			records_h1[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_h1[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("h1", records_h1[i]);
		}
		DBApp.createBitMapIndex("h1","d");
		DBApp.createBitMapIndex("h1","e");
		DBApp.createBitMapIndex("h1","g");
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
		//record31: [a31, b1, c1, d3, e1, f1, g3]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h1", "d", "d2"), ("00100010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("h1", "e", "e1"), ("01000010000100001000010000100001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("h1", "g", "g7"), ("00000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("q0l", cols3);
		//To be indexed: [b, c, e, f, g], to be queried: [b0, c1, e1, f2, g7].
		String [][] records_q0l = new String[9][cols3.length];
		for(int i=0;i<9;i++)
		{
			records_q0l[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_q0l[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("q0l", records_q0l[i]);
		}
		DBApp.createBitMapIndex("q0l","b");
		DBApp.createBitMapIndex("q0l","c");
		DBApp.createBitMapIndex("q0l","e");
		DBApp.createBitMapIndex("q0l","f");
		DBApp.createBitMapIndex("q0l","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("q0l", "b", "b0"), ("101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("q0l", "c", "c1"), ("010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("q0l", "e", "e1"), ("010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("q0l", "f", "f2"), ("001000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("q0l", "g", "g7"), ("000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_21() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 3;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("mc", cols0);
		//To be indexed: [e, g], to be queried: [e2, g7].
		String [][] records_mc = new String[45][cols0.length];
		for(int i=0;i<45;i++)
		{
			records_mc[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_mc[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("mc", records_mc[i]);
		}
		DBApp.createBitMapIndex("mc","e");
		DBApp.createBitMapIndex("mc","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mc", "e", "e2"), ("001000010000100001000010000100001000010000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("mc", "g", "g7"), ("000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("m8q", cols1);
		//To be indexed: [b, d, e, g], to be queried: [b0, d2, e3, g7].
		String [][] records_m8q = new String[9][cols1.length];
		for(int i=0;i<9;i++)
		{
			records_m8q[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_m8q[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("m8q", records_m8q[i]);
		}
		DBApp.createBitMapIndex("m8q","b");
		DBApp.createBitMapIndex("m8q","d");
		DBApp.createBitMapIndex("m8q","e");
		DBApp.createBitMapIndex("m8q","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m8q", "b", "b0"), ("101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m8q", "d", "d2"), ("001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("m8q", "e", "e3"), ("000100001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("m8q", "g", "g7"), ("000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_22() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("q49", cols0);
		//To be indexed: [b, c, g, l, m], to be queried: [b0, c0, g2, l4, m13].
		String [][] records_q49 = new String[16][cols0.length];
		for(int i=0;i<16;i++)
		{
			records_q49[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_q49[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("q49", records_q49[i]);
		}
		DBApp.createBitMapIndex("q49","b");
		DBApp.createBitMapIndex("q49","c");
		DBApp.createBitMapIndex("q49","g");
		DBApp.createBitMapIndex("q49","l");
		DBApp.createBitMapIndex("q49","m");
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
		//record15: [a15, b1, c0, d3, e0, f3, g1, h7, i6, j5, k4, l3, m2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("q49", "b", "b0"), ("1010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("q49", "c", "c0"), ("1001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("q49", "g", "g2"), ("0010000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("q49", "l", "l4"), ("0000100000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("q49", "m", "m13"), ("0000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("j25s", cols1);
		//To be indexed: [c, d, e, i, j, m, o, p, t], to be queried: [c1, d2, e2, i7, j4, m7, o12, p8, t20].
		String [][] records_j25s = new String[29][cols1.length];
		for(int i=0;i<29;i++)
		{
			records_j25s[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_j25s[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("j25s", records_j25s[i]);
		}
		DBApp.createBitMapIndex("j25s","c");
		DBApp.createBitMapIndex("j25s","d");
		DBApp.createBitMapIndex("j25s","e");
		DBApp.createBitMapIndex("j25s","i");
		DBApp.createBitMapIndex("j25s","j");
		DBApp.createBitMapIndex("j25s","m");
		DBApp.createBitMapIndex("j25s","o");
		DBApp.createBitMapIndex("j25s","p");
		DBApp.createBitMapIndex("j25s","t");
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
		//record26: [a26, b0, c2, d2, e1, f2, g5, h2, i8, j6, k4, l2, m0, n12, o11, p10, q9, r8, s7, t6]
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12, p11, q10, r9, s8, t7]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12, q11, r10, s9, t8]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j25s", "c", "c1"), ("01001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j25s", "d", "d2"), ("00100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j25s", "e", "e2"), ("00100001000010000100001000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j25s", "i", "i7"), ("00000001000000001000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j25s", "j", "j4"), ("00001000000000100000000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j25s", "m", "m7"), ("00000001000000000000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j25s", "o", "o12"), ("00000000000010000000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j25s", "p", "p8"), ("00000000100000000000000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("j25s", "t", "t20"), ("00000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("y93o", cols2);
		//To be indexed: [c, d, g], to be queried: [c1, d1, g7].
		String [][] records_y93o = new String[35][cols2.length];
		for(int i=0;i<35;i++)
		{
			records_y93o[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_y93o[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("y93o", records_y93o[i]);
		}
		DBApp.createBitMapIndex("y93o","c");
		DBApp.createBitMapIndex("y93o","d");
		DBApp.createBitMapIndex("y93o","g");
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
		//record34: [a34, b0, c1, d2, e4, f4, g6]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y93o", "c", "c1"), ("01001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y93o", "d", "d1"), ("01000100010001000100010001000100010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("y93o", "g", "g7"), ("00000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("e4u5", cols3);
		//To be indexed: [c, d, e, g, i], to be queried: [c1, d1, e2, g1, i9].
		String [][] records_e4u5 = new String[47][cols3.length];
		for(int i=0;i<47;i++)
		{
			records_e4u5[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_e4u5[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("e4u5", records_e4u5[i]);
		}
		DBApp.createBitMapIndex("e4u5","c");
		DBApp.createBitMapIndex("e4u5","d");
		DBApp.createBitMapIndex("e4u5","e");
		DBApp.createBitMapIndex("e4u5","g");
		DBApp.createBitMapIndex("e4u5","i");
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
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0]
		//record46: [a46, b0, c1, d2, e1, f4, g4, h6, i1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e4u5", "c", "c1"), ("01001001001001001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e4u5", "d", "d1"), ("01000100010001000100010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e4u5", "e", "e2"), ("00100001000010000100001000010000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e4u5", "g", "g1"), ("01000000100000010000001000000100000010000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("e4u5", "i", "i9"), ("00000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t"};
		DBApp.createTable("i19", cols4);
		//To be indexed: [b, c, d, e, g, h, j, l, n, o, q, r, s, t], to be queried: [b0, c0, d1, e1, g1, h3, j7, l8, n5, o4, q2, r11, s0, t20].
		String [][] records_i19 = new String[11][cols4.length];
		for(int i=0;i<11;i++)
		{
			records_i19[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_i19[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("i19", records_i19[i]);
		}
		DBApp.createBitMapIndex("i19","b");
		DBApp.createBitMapIndex("i19","c");
		DBApp.createBitMapIndex("i19","d");
		DBApp.createBitMapIndex("i19","e");
		DBApp.createBitMapIndex("i19","g");
		DBApp.createBitMapIndex("i19","h");
		DBApp.createBitMapIndex("i19","j");
		DBApp.createBitMapIndex("i19","l");
		DBApp.createBitMapIndex("i19","n");
		DBApp.createBitMapIndex("i19","o");
		DBApp.createBitMapIndex("i19","q");
		DBApp.createBitMapIndex("i19","r");
		DBApp.createBitMapIndex("i19","s");
		DBApp.createBitMapIndex("i19","t");
		//first 11 records:
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
		//last 11 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i19", "b", "b0"), ("10101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i19", "c", "c0"), ("10010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i19", "d", "d1"), ("01000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i19", "e", "e1"), ("01000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i19", "g", "g1"), ("01000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i19", "h", "h3"), ("00010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i19", "j", "j7"), ("00000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i19", "l", "l8"), ("00000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i19", "n", "n5"), ("00000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i19", "o", "o4"), ("00001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i19", "q", "q2"), ("00100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i19", "r", "r11"), ("00000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i19", "s", "s0"), ("10000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("i19", "t", "t20"), ("00000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_23() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("g2235", cols0);
		//To be indexed: [b, d, f, g], to be queried: [b0, d1, f1, g7].
		String [][] records_g2235 = new String[40][cols0.length];
		for(int i=0;i<40;i++)
		{
			records_g2235[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_g2235[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("g2235", records_g2235[i]);
		}
		DBApp.createBitMapIndex("g2235","b");
		DBApp.createBitMapIndex("g2235","d");
		DBApp.createBitMapIndex("g2235","f");
		DBApp.createBitMapIndex("g2235","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g2235", "b", "b0"), ("1010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g2235", "d", "d1"), ("0100010001000100010001000100010001000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g2235", "f", "f1"), ("0100000100000100000100000100000100000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("g2235", "g", "g7"), ("0000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("o4b5", cols1);
		//To be indexed: [c, l, n], to be queried: [c0, l2, n14].
		String [][] records_o4b5 = new String[17][cols1.length];
		for(int i=0;i<17;i++)
		{
			records_o4b5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_o4b5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("o4b5", records_o4b5[i]);
		}
		DBApp.createBitMapIndex("o4b5","c");
		DBApp.createBitMapIndex("o4b5","l");
		DBApp.createBitMapIndex("o4b5","n");
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
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4, m3, n2]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o4b5", "c", "c0"), ("10010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("o4b5", "l", "l2"), ("00100000000000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("o4b5", "n", "n14"), ("00000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_24() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("suc8", cols0);
		//To be indexed: [b, c, f, g], to be queried: [b0, c0, f1, g7].
		String [][] records_suc8 = new String[22][cols0.length];
		for(int i=0;i<22;i++)
		{
			records_suc8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_suc8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("suc8", records_suc8[i]);
		}
		DBApp.createBitMapIndex("suc8","b");
		DBApp.createBitMapIndex("suc8","c");
		DBApp.createBitMapIndex("suc8","f");
		DBApp.createBitMapIndex("suc8","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("suc8", "b", "b0"), ("1010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("suc8", "c", "c0"), ("1001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("suc8", "f", "f1"), ("0100000100000100000100"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("suc8", "g", "g7"), ("0000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("bk", cols1);
		//To be indexed: [e, f, g], to be queried: [e2, f1, g7].
		String [][] records_bk = new String[42][cols1.length];
		for(int i=0;i<42;i++)
		{
			records_bk[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_bk[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("bk", records_bk[i]);
		}
		DBApp.createBitMapIndex("bk","e");
		DBApp.createBitMapIndex("bk","f");
		DBApp.createBitMapIndex("bk","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bk", "e", "e2"), ("001000010000100001000010000100001000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bk", "f", "f1"), ("010000010000010000010000010000010000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("bk", "g", "g7"), ("000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("f5r7", cols2);
		//To be indexed: [b, d, e, f, g, h, i], to be queried: [b0, d1, e0, f3, g1, h6, i9].
		String [][] records_f5r7 = new String[46][cols2.length];
		for(int i=0;i<46;i++)
		{
			records_f5r7[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_f5r7[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("f5r7", records_f5r7[i]);
		}
		DBApp.createBitMapIndex("f5r7","b");
		DBApp.createBitMapIndex("f5r7","d");
		DBApp.createBitMapIndex("f5r7","e");
		DBApp.createBitMapIndex("f5r7","f");
		DBApp.createBitMapIndex("f5r7","g");
		DBApp.createBitMapIndex("f5r7","h");
		DBApp.createBitMapIndex("f5r7","i");
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
		//record45: [a45, b1, c0, d1, e0, f3, g3, h5, i0]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f5r7", "b", "b0"), ("1010101010101010101010101010101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f5r7", "d", "d1"), ("0100010001000100010001000100010001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f5r7", "e", "e0"), ("1000010000100001000010000100001000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f5r7", "f", "f3"), ("0001000001000001000001000001000001000001000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f5r7", "g", "g1"), ("0100000010000001000000100000010000001000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("f5r7", "h", "h6"), ("0000001000000010000000100000001000000010000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("f5r7", "i", "i9"), ("0000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_25() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m"};
		DBApp.createTable("fb", cols0);
		//To be indexed: [c, f, g, k, l, m], to be queried: [c0, f4, g3, k0, l6, m13].
		String [][] records_fb = new String[2][cols0.length];
		for(int i=0;i<2;i++)
		{
			records_fb[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_fb[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("fb", records_fb[i]);
		}
		DBApp.createBitMapIndex("fb","c");
		DBApp.createBitMapIndex("fb","f");
		DBApp.createBitMapIndex("fb","g");
		DBApp.createBitMapIndex("fb","k");
		DBApp.createBitMapIndex("fb","l");
		DBApp.createBitMapIndex("fb","m");
		//first 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		//last 2 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fb", "c", "c0"), ("10"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fb", "f", "f4"), ("00"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fb", "g", "g3"), ("00"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fb", "k", "k0"), ("10"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("fb", "l", "l6"), ("00"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("fb", "m", "m13"), ("00"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("mzf5", cols1);
		//To be indexed: [i, k, n], to be queried: [i2, k6, n14].
		String [][] records_mzf5 = new String[39][cols1.length];
		for(int i=0;i<39;i++)
		{
			records_mzf5[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_mzf5[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("mzf5", records_mzf5[i]);
		}
		DBApp.createBitMapIndex("mzf5","i");
		DBApp.createBitMapIndex("mzf5","k");
		DBApp.createBitMapIndex("mzf5","n");
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
		//record35: [a35, b1, c2, d3, e0, f5, g0, h3, i8, j5, k2, l11, m9, n7]
		//record36: [a36, b0, c0, d0, e1, f0, g1, h4, i0, j6, k3, l0, m10, n8]
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7, k4, l1, m11, n9]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8, k5, l2, m12, n10]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mzf5", "i", "i2"), ("001000000001000000001000000001000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("mzf5", "k", "k6"), ("000000100000000001000000000010000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("mzf5", "n", "n14"), ("000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_26() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p"};
		DBApp.createTable("bek1y", cols0);
		//To be indexed: [c, d, j, l, n, o, p], to be queried: [c0, d0, j5, l6, n7, o6, p16].
		String [][] records_bek1y = new String[32][cols0.length];
		for(int i=0;i<32;i++)
		{
			records_bek1y[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_bek1y[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("bek1y", records_bek1y[i]);
		}
		DBApp.createBitMapIndex("bek1y","c");
		DBApp.createBitMapIndex("bek1y","d");
		DBApp.createBitMapIndex("bek1y","j");
		DBApp.createBitMapIndex("bek1y","l");
		DBApp.createBitMapIndex("bek1y","n");
		DBApp.createBitMapIndex("bek1y","o");
		DBApp.createBitMapIndex("bek1y","p");
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
		//record27: [a27, b1, c0, d3, e2, f3, g6, h3, i0, j7, k5, l3, m1, n13, o12, p11]
		//record28: [a28, b0, c1, d0, e3, f4, g0, h4, i1, j8, k6, l4, m2, n0, o13, p12]
		//record29: [a29, b1, c2, d1, e4, f5, g1, h5, i2, j9, k7, l5, m3, n1, o14, p13]
		//record30: [a30, b0, c0, d2, e0, f0, g2, h6, i3, j0, k8, l6, m4, n2, o0, p14]
		//record31: [a31, b1, c1, d3, e1, f1, g3, h7, i4, j1, k9, l7, m5, n3, o1, p15]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bek1y", "c", "c0"), ("10010010010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bek1y", "d", "d0"), ("10001000100010001000100010001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bek1y", "j", "j5"), ("00000100000000010000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bek1y", "l", "l6"), ("00000010000000000010000000000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bek1y", "n", "n7"), ("00000001000000000000010000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("bek1y", "o", "o6"), ("00000010000000000000010000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("bek1y", "p", "p16"), ("00000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_27() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("y5h", cols0);
		//To be indexed: [b, c, e, i, j, k], to be queried: [b0, c0, e0, i5, j1, k11].
		String [][] records_y5h = new String[18][cols0.length];
		for(int i=0;i<18;i++)
		{
			records_y5h[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_y5h[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("y5h", records_y5h[i]);
		}
		DBApp.createBitMapIndex("y5h","b");
		DBApp.createBitMapIndex("y5h","c");
		DBApp.createBitMapIndex("y5h","e");
		DBApp.createBitMapIndex("y5h","i");
		DBApp.createBitMapIndex("y5h","j");
		DBApp.createBitMapIndex("y5h","k");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y5h", "b", "b0"), ("101010101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y5h", "c", "c0"), ("100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y5h", "e", "e0"), ("100001000010000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y5h", "i", "i5"), ("000001000000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("y5h", "j", "j1"), ("010000000001000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("y5h", "k", "k11"), ("000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_28() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("uew", cols0);
		//To be indexed: [b, d, f, l], to be queried: [b0, d0, f4, l12].
		String [][] records_uew = new String[8][cols0.length];
		for(int i=0;i<8;i++)
		{
			records_uew[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_uew[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("uew", records_uew[i]);
		}
		DBApp.createBitMapIndex("uew","b");
		DBApp.createBitMapIndex("uew","d");
		DBApp.createBitMapIndex("uew","f");
		DBApp.createBitMapIndex("uew","l");
		//first 8 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7]
		//last 8 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5]
		//record6: [a6, b0, c0, d2, e1, f0, g6, h6, i6, j6, k6, l6]
		//record7: [a7, b1, c1, d3, e2, f1, g0, h7, i7, j7, k7, l7]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uew", "b", "b0"), ("10101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uew", "d", "d0"), ("10001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uew", "f", "f4"), ("00001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("uew", "l", "l12"), ("00000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("b60f", cols1);
		//To be indexed: [b, g], to be queried: [b0, g7].
		String [][] records_b60f = new String[43][cols1.length];
		for(int i=0;i<43;i++)
		{
			records_b60f[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_b60f[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("b60f", records_b60f[i]);
		}
		DBApp.createBitMapIndex("b60f","b");
		DBApp.createBitMapIndex("b60f","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("b60f", "b", "b0"), ("1010101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("b60f", "g", "g7"), ("0000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"};
		DBApp.createTable("e3", cols2);
		//To be indexed: [b, c, e, f, j, k, l, o, p, q], to be queried: [b0, c0, e3, f2, j3, k3, l9, o0, p4, q17].
		String [][] records_e3 = new String[6][cols2.length];
		for(int i=0;i<6;i++)
		{
			records_e3[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_e3[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("e3", records_e3[i]);
		}
		DBApp.createBitMapIndex("e3","b");
		DBApp.createBitMapIndex("e3","c");
		DBApp.createBitMapIndex("e3","e");
		DBApp.createBitMapIndex("e3","f");
		DBApp.createBitMapIndex("e3","j");
		DBApp.createBitMapIndex("e3","k");
		DBApp.createBitMapIndex("e3","l");
		DBApp.createBitMapIndex("e3","o");
		DBApp.createBitMapIndex("e3","p");
		DBApp.createBitMapIndex("e3","q");
		//first 6 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5]
		//last 6 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0, l0, m0, n0, o0, p0, q0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3, l3, m3, n3, o3, p3, q3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4, l4, m4, n4, o4, p4, q4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5, l5, m5, n5, o5, p5, q5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e3", "b", "b0"), ("101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e3", "c", "c0"), ("100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e3", "e", "e3"), ("000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e3", "f", "f2"), ("001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e3", "j", "j3"), ("000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e3", "k", "k3"), ("000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e3", "l", "l9"), ("000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e3", "o", "o0"), ("100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("e3", "p", "p4"), ("000010"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("e3", "q", "q17"), ("000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("na7m4", cols3);
		//To be indexed: [e, g], to be queried: [e1, g7].
		String [][] records_na7m4 = new String[50][cols3.length];
		for(int i=0;i<50;i++)
		{
			records_na7m4[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_na7m4[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("na7m4", records_na7m4[i]);
		}
		DBApp.createBitMapIndex("na7m4","e");
		DBApp.createBitMapIndex("na7m4","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("na7m4", "e", "e1"), ("01000010000100001000010000100001000010000100001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("na7m4", "g", "g7"), ("00000000000000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		DBApp.createTable("g18", cols4);
		//To be indexed: [g, i, j, l, o, q, r], to be queried: [g1, i7, j7, l4, o11, q6, r18].
		String [][] records_g18 = new String[10][cols4.length];
		for(int i=0;i<10;i++)
		{
			records_g18[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_g18[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("g18", records_g18[i]);
		}
		DBApp.createBitMapIndex("g18","g");
		DBApp.createBitMapIndex("g18","i");
		DBApp.createBitMapIndex("g18","j");
		DBApp.createBitMapIndex("g18","l");
		DBApp.createBitMapIndex("g18","o");
		DBApp.createBitMapIndex("g18","q");
		DBApp.createBitMapIndex("g18","r");
		//first 10 records:
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
		//last 10 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g18", "g", "g1"), ("0100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g18", "i", "i7"), ("0000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g18", "j", "j7"), ("0000000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g18", "l", "l4"), ("0000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g18", "o", "o11"), ("0000000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("g18", "q", "q6"), ("0000001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("g18", "r", "r18"), ("0000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_29() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("jur", cols0);
		//To be indexed: [b, d, f, g], to be queried: [b0, d1, f4, g7].
		String [][] records_jur = new String[27][cols0.length];
		for(int i=0;i<27;i++)
		{
			records_jur[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_jur[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("jur", records_jur[i]);
		}
		DBApp.createBitMapIndex("jur","b");
		DBApp.createBitMapIndex("jur","d");
		DBApp.createBitMapIndex("jur","f");
		DBApp.createBitMapIndex("jur","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jur", "b", "b0"), ("101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jur", "d", "d1"), ("010001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("jur", "f", "f4"), ("000010000010000010000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("jur", "g", "g7"), ("000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_30() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ea8s8", cols0);
		//To be indexed: [e, g], to be queried: [e1, g7].
		String [][] records_ea8s8 = new String[40][cols0.length];
		for(int i=0;i<40;i++)
		{
			records_ea8s8[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_ea8s8[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("ea8s8", records_ea8s8[i]);
		}
		DBApp.createBitMapIndex("ea8s8","e");
		DBApp.createBitMapIndex("ea8s8","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ea8s8", "e", "e1"), ("0100001000010000100001000010000100001000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ea8s8", "g", "g7"), ("0000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_31() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n"};
		DBApp.createTable("idr", cols0);
		//To be indexed: [c, e, f, g, h, i, j, k, l, n], to be queried: [c1, e3, f3, g3, h4, i4, j3, k0, l1, n14].
		String [][] records_idr = new String[12][cols0.length];
		for(int i=0;i<12;i++)
		{
			records_idr[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_idr[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("idr", records_idr[i]);
		}
		DBApp.createBitMapIndex("idr","c");
		DBApp.createBitMapIndex("idr","e");
		DBApp.createBitMapIndex("idr","f");
		DBApp.createBitMapIndex("idr","g");
		DBApp.createBitMapIndex("idr","h");
		DBApp.createBitMapIndex("idr","i");
		DBApp.createBitMapIndex("idr","j");
		DBApp.createBitMapIndex("idr","k");
		DBApp.createBitMapIndex("idr","l");
		DBApp.createBitMapIndex("idr","n");
		//first 12 records:
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
		//last 12 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("idr", "c", "c1"), ("010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("idr", "e", "e3"), ("000100001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("idr", "f", "f3"), ("000100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("idr", "g", "g3"), ("000100000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("idr", "h", "h4"), ("000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("idr", "i", "i4"), ("000010000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("idr", "j", "j3"), ("000100000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("idr", "k", "k0"), ("100000000001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("idr", "l", "l1"), ("010000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("idr", "n", "n14"), ("000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("ss5om", cols1);
		//To be indexed: [c, d, f, g], to be queried: [c0, d0, f3, g7].
		String [][] records_ss5om = new String[26][cols1.length];
		for(int i=0;i<26;i++)
		{
			records_ss5om[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_ss5om[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("ss5om", records_ss5om[i]);
		}
		DBApp.createBitMapIndex("ss5om","c");
		DBApp.createBitMapIndex("ss5om","d");
		DBApp.createBitMapIndex("ss5om","f");
		DBApp.createBitMapIndex("ss5om","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ss5om", "c", "c0"), ("10010010010010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ss5om", "d", "d0"), ("10001000100010001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("ss5om", "f", "f3"), ("00010000010000010000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("ss5om", "g", "g7"), ("00000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j"};
		DBApp.createTable("j1x", cols2);
		//To be indexed: [b, c, f, g, i, j], to be queried: [b0, c0, f0, g5, i2, j10].
		String [][] records_j1x = new String[39][cols2.length];
		for(int i=0;i<39;i++)
		{
			records_j1x[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_j1x[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("j1x", records_j1x[i]);
		}
		DBApp.createBitMapIndex("j1x","b");
		DBApp.createBitMapIndex("j1x","c");
		DBApp.createBitMapIndex("j1x","f");
		DBApp.createBitMapIndex("j1x","g");
		DBApp.createBitMapIndex("j1x","i");
		DBApp.createBitMapIndex("j1x","j");
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
		//record37: [a37, b1, c1, d1, e2, f1, g2, h5, i1, j7]
		//record38: [a38, b0, c2, d2, e3, f2, g3, h6, i2, j8]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j1x", "b", "b0"), ("101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j1x", "c", "c0"), ("100100100100100100100100100100100100100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j1x", "f", "f0"), ("100000100000100000100000100000100000100"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j1x", "g", "g5"), ("000001000000100000010000001000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("j1x", "i", "i2"), ("001000000001000000001000000001000000001"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("j1x", "j", "j10"), ("000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols3 = {"a","b","c","d","e","f","g","h","i"};
		DBApp.createTable("qyhd", cols3);
		//To be indexed: [b, d, e, g, h, i], to be queried: [b0, d2, e1, g0, h6, i9].
		String [][] records_qyhd = new String[12][cols3.length];
		for(int i=0;i<12;i++)
		{
			records_qyhd[i][0] = cols3[0]+i;
			for(int j=1;j<cols3.length;j++)
			{
				records_qyhd[i][j] = cols3[j]+((i%(j+1)));
			}
			DBApp.insert("qyhd", records_qyhd[i]);
		}
		DBApp.createBitMapIndex("qyhd","b");
		DBApp.createBitMapIndex("qyhd","d");
		DBApp.createBitMapIndex("qyhd","e");
		DBApp.createBitMapIndex("qyhd","g");
		DBApp.createBitMapIndex("qyhd","h");
		DBApp.createBitMapIndex("qyhd","i");
		//first 12 records:
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
		//last 12 records:
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qyhd", "b", "b0"), ("101010101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qyhd", "d", "d2"), ("001000100010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qyhd", "e", "e1"), ("010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qyhd", "g", "g0"), ("100000010000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("qyhd", "h", "h6"), ("000000100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("qyhd", "i", "i9"), ("000000000000"));
		//--------------------------------------------------------------------------
		String[] cols4 = {"a","b","c","d","e","f","g","h","i","j","k"};
		DBApp.createTable("uwk7", cols4);
		//To be indexed: [b, d, e, f, i, j, k], to be queried: [b0, d1, e2, f2, i4, j0, k11].
		String [][] records_uwk7 = new String[6][cols4.length];
		for(int i=0;i<6;i++)
		{
			records_uwk7[i][0] = cols4[0]+i;
			for(int j=1;j<cols4.length;j++)
			{
				records_uwk7[i][j] = cols4[j]+((i%(j+1)));
			}
			DBApp.insert("uwk7", records_uwk7[i]);
		}
		DBApp.createBitMapIndex("uwk7","b");
		DBApp.createBitMapIndex("uwk7","d");
		DBApp.createBitMapIndex("uwk7","e");
		DBApp.createBitMapIndex("uwk7","f");
		DBApp.createBitMapIndex("uwk7","i");
		DBApp.createBitMapIndex("uwk7","j");
		DBApp.createBitMapIndex("uwk7","k");
		//first 6 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		//last 6 records:
		//record0: [a0, b0, c0, d0, e0, f0, g0, h0, i0, j0, k0]
		//record1: [a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1]
		//record2: [a2, b0, c2, d2, e2, f2, g2, h2, i2, j2, k2]
		//record3: [a3, b1, c0, d3, e3, f3, g3, h3, i3, j3, k3]
		//record4: [a4, b0, c1, d0, e4, f4, g4, h4, i4, j4, k4]
		//record5: [a5, b1, c2, d1, e0, f5, g5, h5, i5, j5, k5]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uwk7", "b", "b0"), ("101010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uwk7", "d", "d1"), ("010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uwk7", "e", "e2"), ("001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uwk7", "f", "f2"), ("001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uwk7", "i", "i4"), ("000010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("uwk7", "j", "j0"), ("100000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("uwk7", "k", "k11"), ("000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}
	
	
	@Test(timeout = 1000000)
	public void TableInsertionPreIndexing_32() throws Exception
	{
		FileManager.reset();
	
		DBApp.dataPageSize = 5;
		String[] cols0 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("t215", cols0);
		//To be indexed: [g], to be queried: [g7].
		String [][] records_t215 = new String[20][cols0.length];
		for(int i=0;i<20;i++)
		{
			records_t215[i][0] = cols0[0]+i;
			for(int j=1;j<cols0.length;j++)
			{
				records_t215[i][j] = cols0[j]+((i%(j+1)));
			}
			DBApp.insert("t215", records_t215[i]);
		}
		DBApp.createBitMapIndex("t215","g");
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
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("t215", "g", "g7"), ("00000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols1 = {"a","b","c","d","e","f","g"};
		DBApp.createTable("i919", cols1);
		//To be indexed: [b, c, e, g], to be queried: [b0, c1, e1, g7].
		String [][] records_i919 = new String[41][cols1.length];
		for(int i=0;i<41;i++)
		{
			records_i919[i][0] = cols1[0]+i;
			for(int j=1;j<cols1.length;j++)
			{
				records_i919[i][j] = cols1[j]+((i%(j+1)));
			}
			DBApp.insert("i919", records_i919[i]);
		}
		DBApp.createBitMapIndex("i919","b");
		DBApp.createBitMapIndex("i919","c");
		DBApp.createBitMapIndex("i919","e");
		DBApp.createBitMapIndex("i919","g");
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
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i919", "b", "b0"), ("10101010101010101010101010101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i919", "c", "c1"), ("01001001001001001001001001001001001001001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("i919", "e", "e1"), ("01000010000100001000010000100001000010000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("i919", "g", "g7"), ("00000000000000000000000000000000000000000"));
		//--------------------------------------------------------------------------
		String[] cols2 = {"a","b","c","d","e","f","g","h","i","j","k","l"};
		DBApp.createTable("n2ta", cols2);
		//To be indexed: [b, c, d, e, f, i, j, k, l], to be queried: [b0, c0, d0, e1, f1, i1, j1, k7, l12].
		String [][] records_n2ta = new String[17][cols2.length];
		for(int i=0;i<17;i++)
		{
			records_n2ta[i][0] = cols2[0]+i;
			for(int j=1;j<cols2.length;j++)
			{
				records_n2ta[i][j] = cols2[j]+((i%(j+1)));
			}
			DBApp.insert("n2ta", records_n2ta[i]);
		}
		DBApp.createBitMapIndex("n2ta","b");
		DBApp.createBitMapIndex("n2ta","c");
		DBApp.createBitMapIndex("n2ta","d");
		DBApp.createBitMapIndex("n2ta","e");
		DBApp.createBitMapIndex("n2ta","f");
		DBApp.createBitMapIndex("n2ta","i");
		DBApp.createBitMapIndex("n2ta","j");
		DBApp.createBitMapIndex("n2ta","k");
		DBApp.createBitMapIndex("n2ta","l");
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
		//record16: [a16, b0, c1, d0, e1, f4, g2, h0, i7, j6, k5, l4]
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n2ta", "b", "b0"), ("10101010101010101"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n2ta", "c", "c0"), ("10010010010010010"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n2ta", "d", "d0"), ("10001000100010001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n2ta", "e", "e1"), ("01000010000100001"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n2ta", "f", "f1"), ("01000001000001000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n2ta", "i", "i1"), ("01000000001000000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n2ta", "j", "j1"), ("01000000000100000"));
		assertEquals("Bitmap index should be calculated correctly for all values.", DBApp.getValueBits("n2ta", "k", "k7"), ("00000001000000000"));
		assertEquals("Bitmap index should return a sequence of zeros for non-present values.", DBApp.getValueBits("n2ta", "l", "l12"), ("00000000000000000"));
		//--------------------------------------------------------------------------
	
		FileManager.reset();
	}



	private static int genRandNum(int max) 
	{
		Random random = new Random(1520);
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