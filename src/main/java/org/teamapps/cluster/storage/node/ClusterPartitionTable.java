package org.teamapps.cluster.storage.node;

import org.teamapps.cluster.storage.VaultFile;
import org.teamapps.cluster.store.protocol.ClusterPartitionInfo;
import org.teamapps.cluster.store.protocol.NodePartitionInfo;
import org.teamapps.cluster.store.protocol.StorageDevicePartitionInfo;
import org.teamapps.cluster.store.protocol.VaultPartitionInfo;

import java.io.File;
import java.util.*;

public class ClusterPartitionTable {

	private NodePartitionInfo localNodePartitionInfo;
	private String localNodeId;
	private final ClusterPartitionInfo partitionInfo;
	private Map<String, Map<Integer, List<String>>> vaultPartitionNodeMap = new HashMap<>();

	public ClusterPartitionTable(String localNodeId, ClusterPartitionInfo partitionInfo) {
		this.localNodeId = localNodeId;
		this.partitionInfo = partitionInfo;
		if ( localNodeId != null) {
			localNodePartitionInfo = partitionInfo.getNodes().stream().filter(node -> node.getNodeConfig().getNodeId().equals(localNodeId)).findFirst().orElse(null);
		}
		calc();
	}

	private void calc() {
		Map<String, Map<Integer, List<String>>> partitionMap = new HashMap<>();
		for (NodePartitionInfo node : partitionInfo.getNodes()) {
			String nodeId = node.getNodeConfig().getNodeId();
			for (StorageDevicePartitionInfo device : node.getDevices()) {
				for (VaultPartitionInfo vault : device.getVaults()) {
					String vaultId = vault.getVaultId();
					Arrays.stream(vault.getPartitionIds()).forEach(partition -> {
						partitionMap
								.computeIfAbsent(vaultId, s -> new HashMap<>())
								.computeIfAbsent(partition, integer -> new ArrayList<>())
								.add(nodeId);
					});
				}
			}
		}
		this.vaultPartitionNodeMap = partitionMap;

		if (localNodePartitionInfo != null) {
			for (StorageDevicePartitionInfo device : localNodePartitionInfo.getDevices()) {
				String localPath = device.getDeviceConfig().getLocalPath();

			}

		}
	}

	public List<String> getNodes(String vaultId, int partition) {
		return vaultPartitionNodeMap.get(vaultId).get(partition);
	}

	public File getCachePath(String vaultId, VaultFile vaultFile) {

		return null;
	}

	public File getStorePath(String vaultId, VaultFile vaultFile) {
		return null;
	}


}
