package net.soliddesign.snap;

import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import sun.awt.image.ImageWatched.Link;

import com.sun.jna.Native;
import com.sun.jna.Platform;

public class Snap {
  interface Command extends Serializable {
    public void execute(ObjectOutput out) throws IOException;
  }

  static public class Server {
    static public void main(String[] a) throws Exception {
      ObjectInput in = new ObjectInputStream(System.in);
      ObjectOutput out = new ObjectOutputStream(System.out);
      while (in.available() > -1) {
        ((Command) in.readObject()).execute(out);
      }
    }
  }

  public static class CopyRequest implements Command {
    private static final long serialVersionUID = 1L;
    Link link;

    CopyRequest(Link link) {
      this.link = link;
    }

    @Override
    public void execute(ObjectOutput out) throws IOException {
      out.writeObject(new Copy(link.hash));
      out.writeObject(link);
    }
  }

  static public class Link implements Command {
    private static final long serialVersionUID = 1L;
    private File file, hash;

    public Link(File oldFile, File newFile) {
      hash = oldFile;
      file = newFile;
    }

    public void execute(ObjectOutput out) throws IOException {
      if (!hash.exists()) {
        out.writeObject(new CopyRequest(this));
      } else {
        if (Platform.isWindows()) {
          // CLibrary.CreateHardLink(newFile.getPath(), oldFile.getPath(),
          // null);
        } else {
          int linkError = CLibrary.link(hash.getPath(), file.getPath());
          if (linkError != 0) {
            System.err.println("link failed:" + linkError + " from " + hash + " to " + file);
          }
        }
      }
    }
  }

  static class Copy implements Command {
    private static final long serialVersionUID = 1L;
    private File hash;
    // FIXME cheating
    static long copied = 0;

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
      out.writeObject(hash);
      FileInputStream in = new FileInputStream(hash);
      copy(in, out);
      in.close();
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
      hash = (File) in.readObject();
      FileOutputStream out = new FileOutputStream(hash);
      copied += copy(in, out);
      out.close();
    }

    // FIXME, can this be removed?
    private void readObjectNoData() throws ObjectStreamException {
      throw new StreamCorruptedException();
    }

    public Copy(File hash) {
      this.hash = hash;
    }

    public void execute(ObjectOutput out) throws IOException {
      // already done
    }
  }

  static public class CLibrary {
    static {
      Native.register(Platform.isWindows() ? "msvcrt" : "c");
    }

    /** FIXME untested */
    // public static native boolean CreateHardLink(String lpFileName, String
    // lpExistingFileName, byte[] lpSecurityAttributes);

    public static native int link(String from, String to);
  }

  /**
   * should be a multiple of the block size of file system. Used for digesting
   * and copying.
   */
  static final private int BLOCK_SIZE = 1024 * 32;

  /** ensure that a directory exists */
  static private void ensure(File d) {
    if (!d.exists()) {
      d.mkdirs();
    }
  }

  public static void main(String[] argv) throws Exception {
    if (argv.length < 2) {
      System.err.println("Usage: java " + Snap.class.getName() + "base_dir ssh_id snap_dir [label]");
    }
    String id = argv[1];
    File snap_dir = new File(argv[2]);
    String label = argv.length == 3 ? argv[3] : String.format("%1$tY.%1$tm.%1$td.%1$tH.%1$tM.%1$tS", System.currentTimeMillis());
    File base_dir = new File(argv[0]);
    File label_dir = new File(snap_dir, label);
    if (label_dir.exists()) {
      throw new Error(label_dir + " already exists.");
    }
    new Snap(id, snap_dir, label).snap(base_dir);
  }

  /** base of all the snapshots */
  private File baseDir;

  /** number of outstanding directories to be scanned. Do not exit until zero. */
  private int count = 0;

  /** bytes digested */
  private long digested = 0;

  /** directory in baseDir of the data named by hash value (content addressable) */
  private File hashDir;

  private Set<String> hashes = new HashSet<String>();

  private ExecutorService queue = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

  /** snapshot directory */
  private File snapDir;

  private ObjectOutput out;

  public Snap(String id, File base, String label) throws IOException {
    baseDir = base;
    hashDir = new File(baseDir, "hash");
    ensure(hashDir);
    snapDir = new File(base, label);
    ensure(snapDir);
    hashes.addAll(Arrays.asList(hashDir.list()));
    // FIXME don't exit until ssh exits.
    Process p = ssh(id, Server.class, null);
    out = new ObjectOutputStream(p.getOutputStream());
    final ObjectInput in = new ObjectInputStream(p.getInputStream());
    new Thread() {
      public void run() {
        try {
          while (in.available() >= 0) {
            Command c = (Command) in.readObject();
            synchronized (out) {
              c.execute(out);
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }.start();
  }

  /** @return hash string of contents of f */
  private String hash(File f) throws IOException {
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
      digested += len;
      digest.update(buf, 0, len);
    }
    in.close();
    byte[] bytes = digest.digest();
    return String.format("%0" + (bytes.length * 2) + "X", new BigInteger(1, bytes));
  }

  synchronized private void snap(File from) throws IOException {
    long last = 0, start = System.currentTimeMillis();
    snap(from, snapDir);
    final int WAIT_TIME = 1000;
    while (count > 0) {
      try {
        wait(WAIT_TIME);
      } catch (InterruptedException e) {
        throw new Error("Interrupted", e);

      }
      long now = System.currentTimeMillis();
      if (now - last > WAIT_TIME) {
        last = now;
        double microseconds = (now - start) * 1000;
        System.err.println("copied: " + Copy.copied / 1000000.0 + " MB " + (Copy.copied / microseconds) + "MB/s    digested: " + digested / 1000000.0 + " MB "
            + (digested / microseconds) + " MB/s");
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
        synchronized (out) {
          // just request the Link. If it is missing, the copy will be
          // requested.
          out.writeObject(new Link(new File(hashDir, hash(from)), new File(to, from.getName())));
        }
      } catch (IOException e) {
        throw new Error("Failed send Link request.", e);
      }
    }
  }

  static public Process ssh(String id, Class<?> c, List<String> params) throws IOException {
    File jar = new File(System.getProperty("java.class.path"));
    List<String> args1 = Arrays.asList("scp", jar.getAbsoluteFile().getName(), id + ":" + "./");
    Process ssh = Runtime.getRuntime().exec(args1.toArray(new String[args1.size()]));
    asyncCopy(ssh.getErrorStream(), System.err);
    asyncCopy(ssh.getInputStream(), System.out);
    int rtn;
    try {
      rtn = ssh.waitFor();
    } catch (InterruptedException e) {
      rtn = 1;
    }
    if (rtn != 0) {
      System.err.println("failed to copy jar:" + rtn + "\t" + args1);
    }
    List<String> args = new ArrayList<String>(Arrays.asList("ssh", id, "java", "-cp", "./" + "/" + jar.getName()));
    args.addAll(params);
    return Runtime.getRuntime().exec(args.toArray(new String[args.size()]));
  }

  private static long copy(InputStream in, OutputStream out) throws IOException {
    byte[] buf = new byte[BLOCK_SIZE];
    int len;
    long copied = 0;
    while ((len = in.read(buf)) >= 0) {
      out.write(buf, 0, len);
      copied += len;
    }
    return copied;
  }

  private static void asyncCopy(final InputStream in, final OutputStream out) {
    new Thread() {
      public void run() {
        try {
          copy(in, out);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }.start();
  }
}