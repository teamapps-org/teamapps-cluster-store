package org.teamapps.cluster.storage.node;

import org.teamapps.cluster.core.Cluster;
import org.teamapps.cluster.storage.VaultFile;
import org.teamapps.cluster.store.protocol.ClusterFile;
import org.teamapps.cluster.store.protocol.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ClusterStorageNode extends AbstractClusterFileSystemService {

	private final File basePath;
	private final ClusterStorageLeaderClient storageLeaderClient;
	private final Cluster cluster;
	private StorageNodeConfig storageNodeConfig;
	private ClusterStorageLeader storageLeader;
	private NodePartitionInfo nodePartitionInfo;

	public ClusterStorageNode(File basePath, Cluster cluster, StorageNodeConfig storageNodeConfig) throws IOException {
		super(cluster);
		this.basePath = basePath;
		this.cluster = cluster;
		this.storageNodeConfig = storageNodeConfig;
		if (!storageNodeConfig.getNodeId().equals(cluster.getLocalNode().getNodeId())) {
			throw new RuntimeException("Wrong nodeId in config:" + storageNodeConfig.getNodeId() + ", expected:" + cluster.getLocalNode().getNodeId());
		}
		this.nodePartitionInfo = checkAvailableFiles(storageNodeConfig);
		this.storageLeaderClient = new ClusterStorageLeaderClient(cluster);
		if (cluster.isLeaderNode()) {
			startStorageLeader();
		} else {
			cluster.onLeaderAvailable.addListener(clusterNodeData -> {
				if (cluster.isLeaderNode()) {
					startStorageLeader();
				} else {
					registerOnLeader();
				}
			});
		}
	}

	private NodePartitionInfo checkAvailableFiles(StorageNodeConfig storageNodeConfig) {
		NodePartitionInfo nodePartitionInfo = new NodePartitionInfo().setNodeConfig(storageNodeConfig);
		for (StorageDeviceConfig storageDevice : storageNodeConfig.getStorageDevices()) {
			StorageDevicePartitionInfo devicePartitionInfo = new StorageDevicePartitionInfo().setDeviceConfig(storageDevice);
			nodePartitionInfo.addDevices(devicePartitionInfo);
			String localPath = storageDevice.getLocalPath();
			File basePath = new File(localPath);
			if (!basePath.exists()) {
				throw new RuntimeException("Error storage path not available:" + localPath);
			}
			for (File vaultDir : getMatchingFiles(basePath, this::isVaultDir)) {
				VaultPartitionInfo vaultPartitionInfo = new VaultPartitionInfo().setVaultId(vaultDir.getName());
				List<PartitionData> partitionDataList = new ArrayList<>();
				for (File mainPartition : getMatchingFiles(vaultDir, VaultFile::isVaultMainPartition)) {
					for (File subPartition : getMatchingFiles(mainPartition, VaultFile::isVaultSubPartition)) {
						PartitionData partitionData = new PartitionData();
						for (File cfsFile : getMatchingFiles(subPartition, VaultFile::isVaultFile)) {
							long length = cfsFile.length();
							VaultFile vaultFile = new VaultFile(cfsFile);
							if (vaultFile.getLength() == length) {
								partitionData.partitionId = vaultFile.getVirtualPartition();
								partitionData.fileCount++;
								partitionData.fileSize += length;
							}
						}
						if (partitionData.fileCount > 0) {
							partitionDataList.add(partitionData);
						}
					}
				}
				if (!partitionDataList.isEmpty()) {
					int len = partitionDataList.size();
					int[] partitionIds = new int[len];
					long[] partitionSize = new long[len];
					int[] partitionFileCount = new int[len];
					for (int i = 0; i < partitionDataList.size(); i++) {
						PartitionData partitionData = partitionDataList.get(i);
						partitionIds[i] = partitionData.partitionId;
						partitionSize[i] = partitionData.fileSize;
						partitionFileCount[i] = partitionData.fileCount;
					}
					vaultPartitionInfo.setPartitionIds(partitionIds);
					vaultPartitionInfo.setPartitionSize(partitionSize);
					vaultPartitionInfo.setPartitionFileCount(partitionFileCount);
					devicePartitionInfo.addVaults(vaultPartitionInfo);
				}
			}
		}
		return nodePartitionInfo;
	}

	private static class PartitionData {
		public int partitionId;
		public int fileCount;
		private long fileSize;
	}

	private List<File> getMatchingFiles(File file, Predicate<File> predicate) {
		File[] files = file.listFiles();
		if (files != null && files.length != 0) {
			return Arrays.stream(files).filter(predicate).collect(Collectors.toList());
		} else {
			return Collections.emptyList();
		}
	}

	private boolean isVaultDir(File dir) {
		return Arrays.stream(dir.listFiles()).anyMatch(VaultFile::isVaultMainPartition);
	}


	private void registerOnLeader() {
		storageLeaderClient.updateStorageNode(nodePartitionInfo);
	}

	private void startStorageLeader() {
		storageLeader = new ClusterStorageLeader(cluster);
		storageLeader.updateStorageNode(nodePartitionInfo);
	}


	public void addVault(VaultConfig vaultConfig) {
		//todo send to leader.. or wait until available
	}


	@Override
	public ClusterFile retrieveClusterFile(ClusterFileDescriptor value) {
		VaultFile vaultFile = new VaultFile(value.getDescriptor());
		File localPath = getLocalPath(value.getVaultId(), vaultFile, false);
		if (localPath.exists() && localPath.length() == vaultFile.getLength()) {
			return new ClusterFile().setVaultId(value.getVaultId()).setDescriptor(value.getDescriptor()).setFile(localPath);
		} else {
			throw new RuntimeException("Error missing cluster file:" + vaultFile.getDescriptor());
		}
	}

	@Override
	public ClusterFileDescriptor storeClusterFile(ClusterFile value) {
		try {
			VaultFile vaultFile = new VaultFile(value.getDescriptor());
			File localPath = getLocalPath(value.getVaultId(), vaultFile, true);
			value.getFile().copyToFile(localPath);
			if (localPath.length() != vaultFile.getLength()) {
				throw new RuntimeException("Error wrong cluster file length:" + localPath.length() + " vs " + vaultFile.getLength());
			}
			return new ClusterFileDescriptor().setVaultId(value.getVaultId()).setDescriptor(value.getDescriptor());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public ClusterFileDescriptor checkClusterFileIntegrity(ClusterFileDescriptor value) {
		VaultFile vaultFile = new VaultFile(value.getDescriptor());
		File localPath = getLocalPath(value.getVaultId(), vaultFile, false);
		if (!VaultFile.verify(localPath, true)) {
			//todo local file is corrupt -> retrieve new version from other node...
		}
		return value;
	}

	@Override
	public void handlePartitionUpdate(ClusterPartitionInfo clusterPartitionInfo) {

	}

	private File getLocalPath(String vaultId, VaultFile vaultFile, boolean ensureDirectories) {
		File path = new File(basePath, vaultId);
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
