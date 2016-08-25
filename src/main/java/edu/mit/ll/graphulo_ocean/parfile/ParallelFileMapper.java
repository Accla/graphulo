package edu.mit.ll.graphulo_ocean.parfile;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


/**
 * For each input file, attempt to claim it by creating a ".claim" file in [lockDirectory].
 * If the claim is successful, run the [action] on the file.
 * Otherwise some other process claimed the file.
 *
 * Creates [lockDirectory] on construction if it does not exist.
 *
 * Imported from another project. Rest assured this code was tested in the other project.
 */
public final class ParallelFileMapper implements Runnable {

  /**
   * An action to take on each file in a directory.
   */
  public interface FileAction {
    void run(File file);
  }

  private final Iterator<File> inputFiles;
  private final File lockDirectory;
  private final FileAction action;
  
  public Iterator getInputFiles() {
    return this.inputFiles;
  }

  @Override
  public void run() {
    while(this.inputFiles.hasNext()) {
      this.tryFile(this.inputFiles.next());
    }

  }

  private void tryFile(File file) {
    File claimFile;
    try {
      claimFile = new File(lockDirectory.getCanonicalPath(), file.getName() + ".claim");
      if(claimFile.createNewFile()) {
        this.action.run(file);
        Thread.yield();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  
  public File getLockDirectory() {
    return this.lockDirectory;
  }

  
  public FileAction getAction() {
    return this.action;
  }

  public ParallelFileMapper(List<File> inputFiles, File lockDirectory, FileAction action) {
    super();
    this.lockDirectory = lockDirectory;
    this.action = action;
    if(!this.lockDirectory.exists()) {
      try {
        Thread.sleep((long)(Math.random() * 1000));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      if(!this.lockDirectory.exists()) {
        //noinspection ResultOfMethodCallIgnored
        this.lockDirectory.mkdirs();
      }
    } else {
      Preconditions.checkArgument(this.lockDirectory.isDirectory() && this.lockDirectory.canRead(), "Problem with lockDirectory " + this.lockDirectory);
    }

    Iterator<File> var5 = inputFiles.iterator();

    File f;
    boolean var10000;
    label46:
    do {
      if(!var5.hasNext()) {
        ArrayList<File> tmp1 = new ArrayList<>(inputFiles);
        Collections.shuffle((List)tmp1);
        this.inputFiles = tmp1.iterator();
        return;
      }

      f = var5.next();
      Iterator<File> var9 = inputFiles.iterator();

      File it;
      do {
        if(!var9.hasNext()) {
          var10000 = false;
          continue label46;
        }

        it = var9.next();
      } while(!it.getName().equals(f.getName()) || it == f);

      var10000 = true;
    } while(!var10000);

    throw new IllegalArgumentException("One of the input files has the same name as another: " + f.getName());
  }
}
