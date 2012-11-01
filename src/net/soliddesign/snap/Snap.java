package net.soliddesign.snap;

import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sun.jna.*;

public class Snap {
  public static interface CLibrary extends Library {
    CLibrary INSTANCE = (CLibrary) Native.loadLibrary((Platform.isWindows() ? "msvcrt" : "c"), CLibrary.class);

    /** FIXME untested */
    // boolean CreateHardLink(String lpFileName, String lpExistingFileName,
    // Object lpSecurityAttributes);

    int link(String from, String to);

    int __lxstat64(int ver, String path, Stat stat);
  }

  public static class Timespec extends Structure {
    public long tv_sec;
    public long tv_usec;
  }

  public static class Stat extends Structure {
    public long st_dev; /* Device. */
    public long st_ino; /* File serial number. */
    public int st_mode; /* File mode. */
    public int st_nlink; /* Link count. */
    public int st_uid; /* User ID of the file's owner. */
    public int st_gid; /* Group ID of the file's group. */
    public long st_rdev; /* Device number, if device. */
    public long __pad1;
    public long st_size; /* Size of file, in bytes. */
    public int st_blksize; /* Optimal block size for I/O. */
    public int __pad2;
    public long st_blocks; /* Number 512-byte blocks allocated. */
    public long st_atime; /* Time of last access. */
    public long st_atime_nsec;
    public long st_mtime; /* Time of last modification. */
    public long st_mtime_nsec;
    public long st_ctime; /* Time of last status change. */
    public long st_ctime_nsec;
    public int __unused4;
    public int __unused5;

    static final int S_IFMT = 00170000;
    static final int S_IFSOCK = 0140000;
    static final int S_IFLNK = 0120000;
    static final int S_IFREG = 0100000;
    static final int S_IFBLK = 0060000;
    static final int S_IFDIR = 0040000;
    static final int S_IFCHR = 0020000;
    static final int S_IFIFO = 0010000;
    static final int S_ISUID = 0004000;
    static final int S_ISGID = 0002000;
    static final int S_ISVTX = 0001000;

    public Stat(String absolutePath) {
      int r = CLibrary.INSTANCE.__lxstat64(3, absolutePath, this);
      if (r != 0) {
        throw new Error("stat failed:" + r + " errno:" + Native.getLastError());
      }
    }

    final boolean isLink() {
      return ((st_mode & S_IFMT) == S_IFLNK);
    }

    final boolean isReg() {
      return ((st_mode & S_IFMT) == S_IFREG);
    }

    final boolean isDir() {
      return ((st_mode & S_IFMT) == S_IFDIR);
    }

    final boolean isChr() {
      return ((st_mode & S_IFMT) == S_IFCHR);
    }

    final boolean isBlock() {
      return ((st_mode & S_IFMT) == S_IFBLK);
    }

    final boolean isFifo() {
      return ((st_mode & S_IFMT) == S_IFIFO);
    }

    final boolean isSock() {
      return ((st_mode & S_IFMT) == S_IFSOCK);
    }

  }

  /** should correspond to block size of file system. Used for copying. */
  static final int BLOCK_SIZE = 1024 * 32;

  /** ensure that a directory exists */
  static private void ensure(File d) {
    if (!d.exists()) {
      d.mkdirs();
    }
  }

  /** @return hash string of contents of f */
  private static String hash(File f) throws IOException {
    MessageDigest digest;
    InputStream in = null;
    try {
      in = new BufferedInputStream(new FileInputStream(f));
      digest = MessageDigest.getInstance("sha1");
      int len;
      byte[] buf = new byte[BLOCK_SIZE];
      while ((len = in.read(buf)) > 0) {
        digest.update(buf, 0, len);
      }
    } catch (NoSuchAlgorithmException e) {
      throw new Error("Failed to find checksum algorithm.", e);
    } finally {
      if (in != null)
        in.close();
    }
    byte[] bytes = digest.digest();
    return String.format("%0" + (bytes.length * 2) + "X", new BigInteger(1, bytes));
  }

  public static void main(String[] argv) throws Exception {
    if (argv.length < 2) {
      System.err.println("Usage: java " + Snap.class.getName() + " base_dir snap_dir [-v (0-3)] [-label label]");
      System.exit(-1);
    }
    File snap_dir = new File(argv[1]);
    String label = String.format("%1$tY.%1$tm.%1$td.%1$tH.%1$tM.%1$tS", System.currentTimeMillis());
    int v = 0;
    for (int index = 2; index < argv.length; index++) {
      if ("-v".equals(argv[index])) {
        v = Integer.parseInt(argv[++index]);
      } else if ("-label".equals(argv[index])) {
        label = argv[++index];
      }
    }
    File base_dir = new File(argv[0]);
    File label_dir = new File(base_dir, label);
    if (label_dir.exists()) {
      throw new Error(label_dir + " already exists.");
    }
    new Snap(snap_dir, label, v > 0, v > 1, v > 2).snap(base_dir);
  }

  /** base of all the snapshots */
  private File baseDir;

  /** number of outstanding directories to be scanned. Do not exit until zero. */
  private int count = 0;

  /** directory in baseDir of the data named by hash value (content addressable) */
  private File hashDir;

  private boolean logCopy;

  private boolean logLink;

  private boolean logScan;

  private ExecutorService queue = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

  /** snapshot directory */
  private File snapDir;

  public Snap(File base, String label, boolean copy, boolean link, boolean scan) {
    logCopy = copy;
    logLink = link;
    logScan = scan;

    baseDir = base;
    hashDir = new File(baseDir, "hash");
    ensure(hashDir);
    snapDir = new File(base, label);
    ensure(snapDir);
  }

  private void copy(File from, File target) throws IOException {
    if (logCopy) {
      System.err.println("copy: " + from + " to:" + target);
    }
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

  /** copy if not there. Either way, return filename with content. */
  private File copyToRepo(File from) throws IOException {
    File target = new File(hashDir, hash(from));
    if (!target.exists()) {
      copy(from, target);
    }
    return target;
  }

  private void hardlink(File from, File to) {
    if (logLink) {
      System.err.println("link: " + from + " to:" + to);
    }

    // if (Platform.isWindows())
    // CLibrary.INSTANCE.CreateHardLink(to.getPath(), from.getPath(), null);
    // else
    int r = CLibrary.INSTANCE.link(from.getPath(), to.getPath());
    if (r != 0) {
      throw new Error("link failed:" + r + " errno:" + Native.getLastError());
    }
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
    if (logScan) {
      System.err.println("scan: " + from + " to:" + to);
    }
    Stat stat = new Stat(from.getAbsolutePath());

    if (stat.isDir()) {
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
    } else if (stat.isLink()) {
      System.err.println("ignoring link:" + from);
    } else {
      try {
        hardlink(copyToRepo(from), new File(to, from.getName()));
      } catch (IOException e) {
        throw new Error("Failed to copy to repo", e);
      }
    }
  }
}