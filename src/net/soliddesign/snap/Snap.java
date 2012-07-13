package net.soliddesign.snap;

import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sun.jna.*;

public class Snap {
  public interface CLibrary extends Library {
    CLibrary INSTANCE = (CLibrary) Native.loadLibrary((Platform.isWindows() ? "msvcrt" : "c"), CLibrary.class);

    /** FIXME untested */
    boolean CreateHardLink(String lpFileName, String lpExistingFileName, Object lpSecurityAttributes);

    void link(String from, String to);
  }

  /** should correspond to block size of file system. Used for copying. */
  static final int BLOCK_SIZE = 1024 * 32;

  static private void copy(File from, File target) throws IOException {
    InputStream inStream = new FileInputStream(from);
    OutputStream outStream = new FileOutputStream(target);
    byte[] buf = new byte[BLOCK_SIZE];
    int l;
    while ((l = inStream.read(buf)) >= 0) {
      outStream.write(buf, 0, l);
    }
    inStream.close();
    outStream.close();
  }

  /** ensure that a directory exists */
  static private void ensure(File d) {
    if (!d.exists()) {
      d.mkdirs();
    }
  }

  static private void hardlink(File from, File to) {
    if (Platform.isWindows())
      CLibrary.INSTANCE.CreateHardLink(to.getPath(), from.getPath(), null);
    else
      CLibrary.INSTANCE.link(from.getPath(), to.getPath());
  }

  /** @return hash string of contents of f */
  private static String hash(File f) throws IOException {
    InputStream in = new BufferedInputStream(new FileInputStream(f));
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("sha1");
    } catch (NoSuchAlgorithmException e) {
      throw new Error("Failed to find checksum algorithm.", e);
    }
    int len;
    byte[] buf = new byte[BLOCK_SIZE];
    while ((len = in.read(buf)) > 0) {
      digest.update(buf, 0, len);
    }
    in.close();
    byte[] bytes = digest.digest();
    return String.format("%0" + (bytes.length * 2) + "X", new BigInteger(1, bytes));
  }

  public static void main(String[] argv) throws Exception {
    if (argv.length < 2) {
      System.err.println("Usage: java " + Snap.class.getName() + " base_dir snap_dir [label]");
    }
    File snap_dir = new File(argv[1]);
    String label = argv.length == 3 ? argv[2] : String.format("%1$tY.%1$tm.%1$td.%1$tH.%1$tM.%1$tS", System.currentTimeMillis());
    File base_dir = new File(argv[0]);
    File label_dir = new File(base_dir, label);
    if (label_dir.exists()) {
      throw new Error(label_dir + " already exists.");
    }
    new Snap(snap_dir, label).snap(base_dir);
  }

  /** base of all the snapshots */
  private File baseDir;

  /** number of outstanding directories to be scanned. Do not exit until zero. */
  private int count = 0;

  /** directory in baseDir of the data named by hash value (content addressable) */
  private File hashDir;

  /** snapshot directory */
  private File snapDir;

  private ExecutorService queue = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

  public Snap(File base, String label) {
    baseDir = base;
    hashDir = new File(baseDir, "hash");
    ensure(hashDir);
    snapDir = new File(base, label);
    ensure(snapDir);
  }

  /** copy if not there. Either way, return filename with content. */
  private File copyToRepo(File from) throws IOException {
    File target = new File(hashDir, hash(from));
    if (!target.exists()) {
      copy(from, target);
    }
    return target;
  }

  synchronized private void snap(File from) throws IOException {
    snap(from, snapDir);
    while (count > 0) {
      try {
        wait();
      } catch (InterruptedException e) {
        throw new Error("Interrupted", e);
      }
    }
    queue.shutdown();
  }

  /**
   * recursively copy files into repo or hardlink to existing files in
   * repo/hash.
   */
  private void snap(File from, File to) {
    if (from.isDirectory()) {
      final File toDir = new File(to, from.getName());
      ensure(toDir);

      synchronized (Snap.this) {
        for (final File file : from.listFiles()) {
          count++;
          queue.execute(new Runnable() {
            @Override
            public void run() {
              snap(file, toDir);
              synchronized (Snap.this) {
                count--;
                Snap.this.notifyAll();
              }
            }
          });
        }
      }
    } else {
      try {
        hardlink(copyToRepo(from), new File(to, from.getName()));
      } catch (IOException e) {
        throw new Error("Failed to copy to repo", e);
      }
    }
  }
}