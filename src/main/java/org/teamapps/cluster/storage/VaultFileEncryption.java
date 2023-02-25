package org.teamapps.cluster.storage;

import org.teamapps.cluster.crypto.HexUtil;
import org.teamapps.cluster.crypto.ShaHash;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.nio.file.Files;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Arrays;

public class VaultFileEncryption {

	private final File inputFile;
	private final File outputFile;
	private final boolean encryptMode;
	private String passwordHash;
	private String encryptedFileHash;

	public static VaultFileEncryption encryptFile(File inputFile) {
		return new VaultFileEncryption(inputFile, null);
	}

	public static VaultFileEncryption encryptFile(File inputFile, File outputFile) {
		return new VaultFileEncryption(inputFile, outputFile);
	}

	public static VaultFileEncryption decryptFile(File inputFile, File outputFile, String passwordHash) {
		return new VaultFileEncryption(inputFile, outputFile, passwordHash);
	}

	public static VaultFileEncryption decryptFile(File inputFile, String passwordHash) {
		return new VaultFileEncryption(inputFile, null, passwordHash);
	}

	public static String createFileHash(File file) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] buffer = new byte[8192];
			int read;
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
			while ((read = bis.read(buffer)) > 0) {
				digest.update(buffer, 0, read);
			}
			bis.close();
			byte[] hash = digest.digest();
			return HexUtil.bytesToHex(hash);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private VaultFileEncryption(File inputFile, File outputFile) {
		this.inputFile = inputFile;
		this.outputFile = outputFile != null ? outputFile : createTempFile();
		this.encryptMode = true;
		encrypt();
	}

	private VaultFileEncryption(File inputFile, File outputFile, String passwordHash) {
		this.inputFile = inputFile;
		this.outputFile = outputFile != null ? outputFile : createTempFile();
		this.passwordHash = passwordHash;
		this.encryptMode = false;
		decrypt();
	}

	private File createTempFile() {
		try {
			return File.createTempFile("tmp", ".tmp");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void encrypt() {
		try {
			Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			passwordHash = createFileHash(inputFile);
			byte[] iv = new byte[16];
			byte[] key = HexUtil.hexToBytes(passwordHash);
			Arrays.copyOfRange(key, 0, iv.length);
			SecretKeySpec keySpec = new SecretKeySpec(key, "AES");
			IvParameterSpec ivSpec = new IvParameterSpec(iv);
			cipher.init(Cipher.ENCRYPT_MODE, keySpec, ivSpec);
			DigestOutputStream digestOutputStream = new DigestOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile)), digest);
			CipherOutputStream cipherOutputStream = new CipherOutputStream(digestOutputStream, cipher);
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(inputFile));
			byte[] buf = new byte[8096];
			int read = 0;
			while ((read = bis.read(buf)) >= 0) {
				cipherOutputStream.write(buf, 0, read);
			}
			cipherOutputStream.close();
			bis.close();
			encryptedFileHash = HexUtil.bytesToHex(digest.digest());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	private void decrypt() {
		try {
			Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			DigestInputStream digestInputStream = new DigestInputStream(new BufferedInputStream(new FileInputStream(inputFile)), digest);
			byte[] iv = new byte[16];
			byte[] key = HexUtil.hexToBytes(passwordHash);
			Arrays.copyOfRange(key, 0, iv.length);
			SecretKeySpec keySpec = new SecretKeySpec(key, "AES");
			IvParameterSpec ivSpec = new IvParameterSpec(iv);
			cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec);
			CipherInputStream cipherInputStream = new CipherInputStream(digestInputStream, cipher);
			BufferedInputStream bis = new BufferedInputStream(cipherInputStream);
			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFile));
			byte[] buf = new byte[8096];
			int numRead = 0;
			while ((numRead = bis.read(buf)) >= 0) {
				bos.write(buf, 0, numRead);
			}
			bos.close();
			bis.close();
			encryptedFileHash = HexUtil.bytesToHex(digest.digest());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public boolean verifyDecryption(String encryptedFileHash) {
		if (encryptMode) {
			throw new RuntimeException("Wrong mode for file hash verification!");
		}
		return encryptedFileHash.equalsIgnoreCase(this.encryptedFileHash);
	}

	public VaultFile getVaultFile() {
		if (!encryptMode) {
			throw new RuntimeException("Wrong mode for vault file creation!");
		}
		return new VaultFile(outputFile.length(), encryptedFileHash);
	}

	public File getInputFile() {
		return inputFile;
	}

	public File getOutputFile() {
		return outputFile;
	}

	public String getPasswordHash() {
		return passwordHash;
	}

	public String getEncryptedFileHash() {
		return encryptedFileHash;
	}

	public boolean isEncryptMode() {
		return encryptMode;
	}
}
