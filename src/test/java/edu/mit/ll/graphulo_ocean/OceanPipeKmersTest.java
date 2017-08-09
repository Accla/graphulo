package edu.mit.ll.graphulo_ocean;

import edu.mit.ll.graphulo.examples.ExampleUtil;
import org.apache.commons.io.output.NullOutputStream;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
@RunWith(Parameterized.class)
public class OceanPipeKmersTest {

  private static int numlines = -1;
  private static final File file = ExampleUtil.getDataFile("S0002_11_cnt_cut.csv");
  private final OceanPipeKmers.ReadMode mode;
  private final boolean binary;

  public OceanPipeKmersTest(OceanPipeKmers.ReadMode mode, boolean binary) {
    if (numlines == -1) {
      File file = ExampleUtil.getDataFile("S0002_11_cnt_cut.csv");
      // count the number of lines in file
      int numlines = 0;
      try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
        String l;
        while ((l = reader.readLine()) != null)
          if (!l.trim().isEmpty())
            numlines++;
      } catch (IOException e) {
        throw new RuntimeException("problem reading example file: "+file, e);
      }
    }
    this.mode = mode;
    this.binary = binary;
  }

  @Test
  public void executeNormal() throws Exception {

  }

  @Parameterized.Parameters(name = "test {index}: Mode={0} binary={1}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> list = new ArrayList<>();
    for (OceanPipeKmers.ReadMode mode : OceanPipeKmers.ReadMode.values()) {
      list.add(new Object[] { mode, false });
      list.add(new Object[] { mode, true });
    }
    return list;
  }


  @Test
  public void execute() throws Exception {
    String[] args;
    {
      List<String> list = new ArrayList<>();
      list.add("-K");
      list.add("11");
      switch (mode) {
        case FORWARD:
          break;
        case RC:
          list.add("-rc");
          break;
        case LEX:
          list.add("-lex");
          break;
        default: throw new AssertionError();
      }
      if (binary)
        list.add("-binary");
      args = list.toArray(new String[0]);
    }

    // count the number of lines in file
    int numlines = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      String l;
      while ((l = reader.readLine()) != null)
        if (!l.trim().isEmpty())
          numlines++;
    }


    PrintStream out = new PrintStream(new NullOutputStream());
    int linesProcessed;
    try (InputStream in = new FileInputStream(file))  {
      linesProcessed = new OceanPipeKmers().execute(args, in, out);
    }
    Assert.assertEquals(numlines, linesProcessed);
  }

}