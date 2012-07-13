package net.soliddesign.snap;

import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sun.jna.*;

public class Snap {
  /** base of all the snapshots */
  private File baseDir;
  /** directory in baseDir of the data named by hashvalue (content addressable) */
  private File hashDir;
  /** snapshot directory */
  private File snapDir;

  public Snap(File base, String label) {
    baseDir = base;
    hashDir = new File(baseDir, "hash");
    ensure(hashDir);
    snapDir = new File(base, label);
  }

  /** @return hash of contents of f */
  private String hash(File f) throws IOException {
    InputStream in = new BufferedInputStream(new FileInputStream(f));
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("md5");
    } catch (NoSuchAlgorithmException e) {
      throw new Error("Failed to find checksum algorythm.", e);
    }
    int l;
    byte[] buf = new byte[BLOCK_SIZE];
    while ((l = in.read(buf)) > 0) {
      digest.update(buf, 0, l);
    }
    in.close();
    return toHex(digest.digest());
  }

  public static String toHex(byte[] bytes) {
    BigInteger bi = new BigInteger(1, bytes);
    return String.format("%0" + (bytes.length << 1) + "X", bi);
  }

  /** copy if not there. Either way, return filename with content. */
  private File copyToRepo(File from) {
    try {
      String hash = hash(from);
      File base = new File(hashDir, hash);
      int i = 0;
      File target = new File(base + "." + i++);
      while (target.exists()) {
        if (equalishContent(from, target)) {
          return target;
        }
        target = new File(base + "." + i++);
      }
      // ok it doesn't exist, so really copy
      InputStream inStream = new FileInputStream(from);
      OutputStream outStream = new FileOutputStream(target);
      // FIXME replace with a no userspace copy.
      byte[] buf = new byte[4096];
      int l;
      while ((l = inStream.read(buf)) >= 0) {
        outStream.write(buf, 0, l);
      }
      inStream.close();
      outStream.close();
      return target;
    } catch (IOException e) {
      throw new Error("failed to copy: " + from, e);
    }
  }

  private void ensure(File d) {
    if (!d.exists()) {
      d.mkdirs();
    }
  }

  final int BLOCK_SIZE = 4096;

  /**
   * given that the hash is the same, are the files close enough to be
   * considered the same?
   */
  private boolean equalishContent(File a, File b) throws IOException {
    // check for existence and length
    if (!a.exists() || !b.exists() || a.length() != b.length()) {
      return false;
    }
    if (a.length() == 0) {
      return true;
    }
    // Check first block
    InputStream ais = new FileInputStream(a);
    InputStream bis = new FileInputStream(b);
    try {
      byte[] abuf = new byte[BLOCK_SIZE];
      byte[] bbuf = new byte[BLOCK_SIZE];
      int alen, blen;
      alen = ais.read(abuf);
      blen = bis.read(bbuf);
      if (alen != blen || !Arrays.equals(abuf, bbuf)) {
        return false;
      }
      return true;
    } finally {
      ais.close();
      bis.close();
    }
  }

  public static void main(String[] argv) throws Exception {
    if (argv.length < 2) {
      System.err.println("Usage: snap base_dir snap_dir [snap_label]");
    }
    File base = new File(argv[1]);
    String label = argv.length == 3 ? argv[2] : String.format(
        "%1$tY.%1$tm.%1$td.%1$tH.%1$tM.%1$tS", System.currentTimeMillis());
    new Snap(base, label).snap(new File(argv[0]));
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

  ExecutorService queue = Executors.newFixedThreadPool(Runtime.getRuntime()
      .availableProcessors() * 2);
  int count = 0;

  private void snap(File from, File to) {
    ensure(to);
    if (from.isDirectory()) {
      final File toDir = new File(to, from.getName());

      synchronized (Snap.this) {
        for (final File file : from.listFiles()) {
          count++;
          queue.execute(new Runnable() {
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
      hardlink(copyToRepo(from), new File(to, from.getName()));
    }
  }

  public interface CLibrary extends Library {
    CLibrary INSTANCE = (CLibrary) Native.loadLibrary(
        (Platform.isWindows() ? "msvcrt" : "c"), CLibrary.class);

    boolean CreateHardLink(String lpFileName, String lpExistingFileName,
        Object lpSecurityAttributes);

    void link(String from, String to);

  }

  private void hardlink(File from, File to) {
    if (Platform.isWindows())
      CLibrary.INSTANCE.CreateHardLink(to.getPath(), from.getPath(), null);
    else
      CLibrary.INSTANCE.link(from.getPath(), to.getPath());
  }
}
