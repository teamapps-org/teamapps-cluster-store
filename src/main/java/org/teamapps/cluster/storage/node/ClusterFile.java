package org.teamapps.cluster.storage.node;

import org.teamapps.cluster.storage.VaultFile;
import org.teamapps.message.protocol.file.FileData;
import org.teamapps.message.protocol.file.FileDataType;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import static org.teamapps.message.protocol.file.FileDataType.CLUSTER_STORE;

public class ClusterFile implements FileData {

	private final String fileName;
	private final long length;
	private final String descriptor;
	private final String vaultId;
	private final String encryptedFileHash;
	private final boolean encrypted;
	private final String passwordHash;
	private final ClusterFileProvider clusterFileProvider;

	public ClusterFile(String fileName, String vaultId, VaultFile vaultFile, String passwordHash, ClusterFileProvider clusterFileProvider) {
		this.fileName = fileName;
		this.vaultId = vaultId;
		this.encryptedFileHash = vaultFile.getFileHashAsString();
		this.length = vaultFile.getLength();
		this.passwordHash = passwordHash;
		this.descriptor = vaultId + "/" + encryptedFileHash;
		this.encrypted = true;
		this.clusterFileProvider = clusterFileProvider;
	}

	public ClusterFile(FileData fileData, ClusterFileProvider clusterFileProvider) {
		this.clusterFileProvider = clusterFileProvider;
		if (fileData.getType() != CLUSTER_STORE) {
			throw new RuntimeException("Wrong file data type:" + fileData.getType());
		}
		this.fileName = fileData.getFileName();
		this.length = fileData.getLength();
		this.descriptor = fileData.getDescriptor();
		String[] parts = descriptor.split("/");
		this.vaultId = parts[0];
		this.encryptedFileHash = parts[1];
		this.encrypted = fileData.isEncrypted();
		this.passwordHash = fileData.getEncryptionKey();
	}

	public ClusterFile(String fileName, long length, String descriptor, String encryptionKey, ClusterFileProvider clusterFileProvider) {
		this.clusterFileProvider = clusterFileProvider;
		this.fileName = fileName;
		this.length = length;
		this.descriptor = descriptor;
		String[] parts = descriptor.split("/");
		this.vaultId = parts[0];
		this.encryptedFileHash = parts[1];
		this.encrypted = true;
		this.passwordHash = encryptionKey;
	}

	@Override
	public FileDataType getType() {
		return CLUSTER_STORE;
	}

	@Override
	public String getFileName() {
		return fileName;
	}

	@Override
	public long getLength() {
		return length;
	}

	@Override
	public InputStream getInputStream() throws IOException {
		return clusterFileProvider.getClusterFile(vaultId, new VaultFile(length, encryptedFileHash), passwordHash);
	}

	@Override
	public File getAsFile() {
		return null;
	}

	@Override
	public String getDescriptor() {
		return descriptor;
	}

	@Override
	public boolean isEncrypted() {
		return encrypted;
	}

	@Override
	public String getEncryptionKey() {
		return passwordHash;
	}

	@Override
	public String getBasePath() {
		return vaultId;
	}
}
