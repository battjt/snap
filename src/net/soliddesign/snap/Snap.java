package net.soliddesign.snap;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;

public class Snap {
  File hashDir;
  File snapDir;
  File baseDir;

  public Snap(File base, String label) {
    baseDir = base;
    hashDir = new File(baseDir, "hash");
    ensure(hashDir);
    snapDir = new File(base, label);
  }

  private String hash(File f) throws IOException {
    InputStream in = new BufferedInputStream(new FileInputStream(f));
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("md5");
    } catch (NoSuchAlgorithmException e) {
      throw new Error("Failed to find checksum algorythm.", e);
    }
    int l;
    byte[] buf = new byte[4096];
    // FIXME skip around with large files
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

  private void copy(InputStream in, OutputStream out) throws IOException {
    byte[] buf = new byte[4096];
    int l;
    while ((l = in.read(buf)) >= 0) {
      out.write(buf, 0, l);
    }
  }

  private void copy(File in, File out) throws IOException {
    // System.err.println("cp " + in + " " + out);

    BufferedInputStream inStream = new BufferedInputStream(new FileInputStream(
        in));
    BufferedOutputStream outStream = new BufferedOutputStream(
        new FileOutputStream(out));
    copy(inStream, outStream);
    inStream.close();
    outStream.close();
  }

  /** copy if not there. Either way, return filename with content. */
  private File copyToRepo(File from) throws IOException {
    String hash = hash(from);
    File base = new File(hashDir, hash);
    int i = 0;
    File target = new File(base + "." + i++);
    while (target.exists()) {
      if (equalContent(from, target)) {
        return target;
      }
      target = new File(base + "." + i++);
    }
    copy(from, target);
    return target;
  }

  private void ensure(File d) {
    if (!d.exists()) {
      d.mkdirs();
    }
  }

  final int BLOCK_SIZE = 4096;

  private boolean equalContent(File a, File b) throws IOException {
    // check for existence and length
    if (!a.exists() || !b.exists() || a.length() != b.length()) {
      return false;
    }
    // Check last two blocks
    int len = a.length() > BLOCK_SIZE * 10 ? (int) (BLOCK_SIZE + a.length()
        % BLOCK_SIZE) : 0;
    if (len > 0 && !Arrays.equals(readLast(a, len), readLast(b, len)))
      return false;
    // Check rest
    BufferedInputStream ais = new BufferedInputStream(new FileInputStream(a),
        BLOCK_SIZE);
    BufferedInputStream bis = new BufferedInputStream(new FileInputStream(b),
        BLOCK_SIZE);
    try {
      return a.length() == 0 || equalContent(ais, bis, a.length() - len);
    } finally {
      ais.close();
      bis.close();
    }
  }

  private byte[] readLast(File a, int len) throws FileNotFoundException,
      IOException {
    byte[] buf = new byte[len];
    RandomAccessFile aRAF = new RandomAccessFile(a, "r");
    aRAF.seek(a.length() - buf.length);
    aRAF.read(buf);
    return buf;
  }

  private boolean equalContent(InputStream a, InputStream b, long size)
      throws IOException {
    byte[] abuf = new byte[BLOCK_SIZE];
    byte[] bbuf = new byte[BLOCK_SIZE];
    int alen, blen;
    long done = 0;
    do {
      alen = a.read(abuf);
      blen = b.read(bbuf);
      if (alen != blen || !Arrays.equals(abuf, bbuf)) {
        return false;
      }
      done += alen;
    } while (done < size); // FIXME lazy
    return true;
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

  private void snap(File from) throws IOException {
    snap(from, snapDir);
  }

  private void snap(File from, File to) throws IOException {
    if (from.isDirectory()) {
      to = new File(to, from.getName());
      for (File file : from.listFiles()) {
        snap(file, to);
      }
    } else {
      ensure(to);
      to = new File(to, from.getName());
      hardlink(copyToRepo(from), to);
    }
  }

  public interface CLibrary extends Library {
    CLibrary INSTANCE = (CLibrary) Native.loadLibrary(
        (Platform.isWindows() ? "msvcrt" : "c"), CLibrary.class);

    void link(String from, String to);
  }

  /**
   * hard link
   */
  private void hardlink(File from, File to) throws IOException {
    // System.err.println("ln \"" + from + "\" \"" + to);
    // Runtime.getRuntime().exec(
    // new String[] { "ln", from.getAbsolutePath(), to.getAbsolutePath() });
    CLibrary.INSTANCE.link(from.getPath(), to.getPath());
  }
}
