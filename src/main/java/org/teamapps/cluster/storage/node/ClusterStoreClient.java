package org.teamapps.cluster.storage.node;

import org.teamapps.cluster.core.Cluster;
import org.teamapps.cluster.storage.VaultFile;
import org.teamapps.cluster.storage.VaultFileEncryption;
import org.teamapps.cluster.store.protocol.ClusterFileDescriptor;
import org.teamapps.cluster.store.protocol.ClusterFileSystemServiceClient;
import org.teamapps.cluster.store.protocol.ClusterPartitionInfo;
import org.teamapps.message.protocol.file.FileData;
import org.teamapps.message.protocol.file.FileDataReader;
import org.teamapps.message.protocol.file.FileDataType;
import org.teamapps.message.protocol.file.GenericFileData;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ClusterStoreClient implements FileDataReader, ClusterFileProvider {

	private final File cachePath;
	private final ClusterFileSystemServiceClient serviceClient;
	private ClusterPartitionTable clusterPartitionTable;

	public ClusterStoreClient(File cachePath, Cluster cluster) {
		this.cachePath = cachePath;
		this.serviceClient = new ClusterFileSystemServiceClient(cluster);
	}

	public void updatePartitionTable(ClusterPartitionInfo partitionInfo) {
		//clusterPartitionTable = new ClusterPartitionTable(partitionInfo);
	}

	public VaultClusterStore getVaultClusterStore(String vaultId) {
		final ClusterFileProvider clusterFileProvider = this;
		final ClusterStoreClient clusterStoreClient = this; //todo
		return new VaultClusterStore() {
			@Override
			public FileData readFileData(FileDataType type, String fileName, long length, String descriptor, boolean encrypted, String encryptionKey) throws IOException {
				GenericFileData fileData = new GenericFileData(type, fileName, length, descriptor, encrypted, encryptionKey);
				if (fileData.getType() == FileDataType.LOCAL_FILE) {
					return fileData;
				} else {
					return new ClusterFile(fileData, clusterFileProvider);
				}
			}

			@Override
			public FileData writeFileData(FileData fileData) throws IOException {
				return clusterStoreClient.writeClusterFile(vaultId, fileData);
			}
		};
	}

	private FileData writeClusterFile(String vaultId, FileData fileData) throws IOException {
		if (fileData.getType() == FileDataType.CLUSTER_STORE) {
			return fileData;
		} else {
			File inputFile = fileData.copyToTempFile();
			VaultFileEncryption vaultFileEncryption = VaultFileEncryption.encryptFile(inputFile);
			VaultFile vaultFile = vaultFileEncryption.getVaultFile();
			String passwordHash = vaultFileEncryption.getPasswordHash();

			//send to nodes with this partition
			List<String> partitionNodes = clusterPartitionTable.getNodes(vaultId, vaultFile.getVirtualPartition());
			org.teamapps.cluster.store.protocol.ClusterFile clusterFile = new org.teamapps.cluster.store.protocol.ClusterFile()
					.setVaultId(vaultId)
					.setDescriptor(vaultFile.getDescriptor())
					.setFile(vaultFileEncryption.getOutputFile());
			for (String partitionNode : partitionNodes) {
				serviceClient.storeClusterFile(clusterFile, partitionNode);
			}

			//copy unencrypted to cache
			File fileCachePath = createVaultFileCachePath(vaultId, vaultFile, true);
			Files.move(inputFile.toPath(), fileCachePath.toPath());

			return new ClusterFile(fileData.getFileName(), vaultId, vaultFile, passwordHash, this);
		}
	}

	@Override
	public InputStream getClusterFile(String vaultId, VaultFile vaultFile, String password) throws IOException {
		File localCachePath = createVaultFileCachePath(vaultId, vaultFile, true);
		if (localCachePath.exists() && vaultFile.getLength() == localCachePath.length()) {
			return new FileInputStream(localCachePath);
		}

		List<String> partitionNodes = new ArrayList<>(clusterPartitionTable.getNodes(vaultId, vaultFile.getVirtualPartition()));
		Collections.shuffle(partitionNodes);
		ClusterFileDescriptor clusterFileDescriptor = new ClusterFileDescriptor()
				.setVaultId(vaultId)
				.setDescriptor(vaultFile.getDescriptor());
		for (String partitionNode : partitionNodes) {
			org.teamapps.cluster.store.protocol.ClusterFile clusterFile = serviceClient.retrieveClusterFile(clusterFileDescriptor, partitionNode);
			if (clusterFile != null) {
				FileData fileData = clusterFile.getFile();
				//todo handle decryption errors
				VaultFileEncryption decryptedFile = VaultFileEncryption.decryptFile(fileData.copyToTempFile(), localCachePath, password);
				if (decryptedFile.verifyDecryption(vaultFile.getFileHashAsString())) {
					return new FileInputStream(localCachePath);
				} else {
					//todo log error
					serviceClient.checkClusterFileIntegrity(clusterFileDescriptor, partitionNode);
				}
			}
		}
		throw new RuntimeException("Error could not find cluster file:" + vaultId + ", " + vaultFile.getDescriptor());
	}

	@Override
	public FileData readFileData(FileDataType type, String fileName, long length, String descriptor, boolean encrypted, String encryptionKey) throws IOException {
		GenericFileData fileData = new GenericFileData(type, fileName, length, descriptor, encrypted, encryptionKey);
		if (fileData.getType() == FileDataType.LOCAL_FILE) {
			return fileData;
		} else {
			return new ClusterFile(fileData, this);
		}
	}

	private File createVaultFileCachePath(String vaultId, VaultFile vaultFile, boolean ensureDirectories) {
		File path = new File(cachePath, vaultId);
		if (ensureDirectories && !path.exists()) {
			path.mkdir();
		}
		File localPath = vaultFile.getLocalPath(path);
		if (ensureDirectories && !localPath.exists()) {
			localPath.getParentFile().getParentFile().mkdir();
			localPath.getParentFile().mkdir();
		}
		return localPath;
	}

}
