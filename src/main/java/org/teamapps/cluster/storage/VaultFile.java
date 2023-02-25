package org.teamapps.cluster.storage;

import org.teamapps.cluster.crypto.HexUtil;
import org.teamapps.cluster.crypto.ShaHash;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class VaultFile {

	private static final char DESCRIPTOR_PREFIX = 'c';
	private static final String MAIN_PARTITION_PREFIX = "pt";
	private static final String SUB_PARTITION_PREFIX = "sp";
	private static final String FILE_SUFFIX = ".cfs";

	private final String descriptor;

	public static boolean verify(File file, boolean contentHash) {
		if (isVaultFile(file)) {
			VaultFile vaultFile = new VaultFile(file);
			if (vaultFile.getLength() != file.length()) {
				return false;
			} else {
				if (contentHash) {
					String fileHash = VaultFileEncryption.createFileHash(file);
					return vaultFile.getFileHashAsString().equalsIgnoreCase(fileHash);
				} else {
					return true;
				}
			}
		} else {
			return false;
		}
	}

	public static boolean isVaultFile(File file) {
		String name = file.getName();
		return name.length() > 65 && name.startsWith(DESCRIPTOR_PREFIX + "") && name.endsWith(FILE_SUFFIX);
	}

	public static boolean isVaultMainPartition(File file) {
		String name = file.getName();
		return file.isDirectory() && name.startsWith(MAIN_PARTITION_PREFIX) && name.length() == MAIN_PARTITION_PREFIX.length() + 1;
	}

	public static boolean isVaultSubPartition(File file) {
		String name = file.getName();
		return file.isDirectory() && name.startsWith(SUB_PARTITION_PREFIX) && name.length() == SUB_PARTITION_PREFIX.length() + 1;
	}

	public VaultFile(String descriptor) {
		this.descriptor = descriptor.endsWith(FILE_SUFFIX) ? descriptor.substring(0, descriptor.length() - FILE_SUFFIX.length()) : descriptor;
	}

	public VaultFile(File file) {
		this(file.getName());
	}

	public VaultFile(long length, String hash) {
		this.descriptor = (DESCRIPTOR_PREFIX + hash + Long.toString(length, 16)).toLowerCase();
	}

	public String getDescriptor() {
		return descriptor;
	}

	public String getLeafPath() {
		return MAIN_PARTITION_PREFIX + descriptor.charAt(2) + "/" + SUB_PARTITION_PREFIX + descriptor.charAt(3) + "/" + descriptor + FILE_SUFFIX;
	}

	public File getLocalPath(File vaultPath) {
		return new File(vaultPath, getLeafPath());
	}

	public File copyToStore(File basePath, VaultFileEncryption encrypted) {
		return transferToStore(basePath, encrypted, false);
	}

	public File moveToStore(File basePath, VaultFileEncryption encrypted) {
		return transferToStore(basePath, encrypted, true);
	}

	public File transferToStore(File basePath, VaultFileEncryption encrypted, boolean moveFile) {
		try {
			File storePath = getLocalPath(basePath);
			if (!storePath.getParentFile().exists()) {
				storePath.getParentFile().getParentFile().mkdir();
				storePath.getParentFile().mkdir();
			}
			if (moveFile) {
				Files.move(encrypted.getOutputFile().toPath(), storePath.toPath(), StandardCopyOption.REPLACE_EXISTING);
			} else {
				Files.copy(encrypted.getOutputFile().toPath(), storePath.toPath(), StandardCopyOption.REPLACE_EXISTING);
			}
			return storePath;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public long getLength() {
		return Long.parseLong(descriptor.substring(65), 16);
	}

	public int getVirtualPartition() {
		return Integer.parseInt(descriptor.substring(1, 3), 16);
	}

	public String getFileHashAsString() {
		return descriptor.substring(1, 65);
	}

	public byte[] getFileHash() {
		return HexUtil.hexToBytes(getFileHashAsString());
	}

	@Override
	public String toString() {
		return "VaultFile:" + descriptor;
	}

}
