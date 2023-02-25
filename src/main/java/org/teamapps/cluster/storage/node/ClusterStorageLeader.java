package org.teamapps.cluster.storage.node;

import org.teamapps.cluster.core.Cluster;
import org.teamapps.cluster.storage.PartitionTable;
import org.teamapps.cluster.storage.node.calc.PhysicalStorage;
import org.teamapps.cluster.storage.node.calc.Vault;
import org.teamapps.cluster.storage.node.calc.VirtualPartition;
import org.teamapps.cluster.store.protocol.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ClusterStorageLeader extends AbstractClusterStorageLeader {

	private final ClusterFileSystemServiceClient clusterFileSystemServiceClient;
	private Map<String, NodePartitionInfo> nodePartitionInfoMap = new ConcurrentHashMap<>();
	private Map<String, VaultConfig> vaultConfigMap = new ConcurrentHashMap<>();

	private ClusterPartitionInfo partitionInfo;
	private Cluster cluster;

	public ClusterStorageLeader(Cluster cluster) {
		super(cluster);
		this.cluster = cluster;
		clusterFileSystemServiceClient = new ClusterFileSystemServiceClient(cluster);
	}

	@Override
	public ClusterPartitionInfo updateStorageNode(NodePartitionInfo nodePartitionInfo) {
		nodePartitionInfoMap.put(nodePartitionInfo.getNodeConfig().getNodeId(), nodePartitionInfo);


		return null; //todo
	}

	@Override
	public ClusterPartitionInfo updateVault(VaultConfig vaultConfig) {
		vaultConfigMap.put(vaultConfig.getVaultId(), vaultConfig);
		return null;
	}

	private ClusterPartitionInfo calculatePartitions(Collection<NodePartitionInfo> partitionInfos, Collection<VaultConfig> vaultConfigs) {
		Map<String, Vault> vaultById = new HashMap<>();
		List<PhysicalStorage> storages = new ArrayList<>();
		for (NodePartitionInfo nodePartitionInfo : partitionInfos) {
			StorageNodeConfig nodeConfig = nodePartitionInfo.getNodeConfig();
			for (StorageDevicePartitionInfo device : nodePartitionInfo.getDevices()) {
				StorageDeviceConfig deviceConfig = device.getDeviceConfig();
				PhysicalStorage physicalStorage = new PhysicalStorage(
						deviceConfig.getDeviceId(),
						nodeConfig.getNodeId(),
						nodeConfig.getDataCenterId(),
						nodeConfig.getCountryCode(),
						deviceConfig.getCapacity());
				storages.add(physicalStorage);
				for (VaultPartitionInfo vaultInfo : device.getVaults()) {
					String vaultId = vaultInfo.getVaultId();
					Vault vault = vaultById.computeIfAbsent(vaultId, Vault::new);
					int[] partitionIds = vaultInfo.getPartitionIds();
					int[] partitionFileCounts = vaultInfo.getPartitionFileCount();
					long[] partitionSizes = vaultInfo.getPartitionSize();
					for (int i = 0; i < partitionIds.length; i++) {
						int partitionId = partitionIds[i];
						long partitionSize = partitionSizes[i];
						int partitionFileCount = partitionFileCounts[i];
						vault.addPartition(partitionId, partitionSize, partitionFileCount, physicalStorage);
					}
				}
			}
		}

		for (VaultConfig vaultConfig : vaultConfigs) {
			Vault vault = vaultById.computeIfAbsent(vaultConfig.getVaultId(), Vault::new);
			vault.createMissingPartitions(vaultConfig.getRequiredFileCopies());
			vault.setMinVaultSize(vaultConfig.getMinVaultSize());
			vault.setPriority(vaultConfig.getPriority());
		}

		List<Vault> vaults = vaultById.values().stream().toList();
		PartitionTable partitionTable = new PartitionTable(vaults, storages);
		partitionTable.calculatePartitions();

		Map<String, NodePartitionInfo> nodePartitionInfoByNodeId
				= partitionInfos.stream().collect(Collectors.toMap(nodePartitionInfo -> nodePartitionInfo.getNodeConfig().getNodeId(), nodePartitionInfo -> nodePartitionInfo));
		ClusterPartitionInfo clusterPartitionInfo = new ClusterPartitionInfo();
		clusterPartitionInfo.setLeaderNodeId(cluster.getLeaderNode().getNodeId());
		clusterPartitionInfo.setNodes(new ArrayList<>(partitionInfos));

		for (PhysicalStorage storage : storages) {
			NodePartitionInfo nodePartitionInfo = nodePartitionInfoByNodeId.get(storage.getNodeId());
			StorageDevicePartitionInfo devicePartitionInfo = nodePartitionInfo.getDevices().stream().filter(d -> d.getDeviceConfig().getDeviceId().equals(storage.getDiscId())).findFirst().orElse(null);
			Map<String, List<VirtualPartition>> partitionByVaultId = storage.getVirtualPartitions().stream().collect(Collectors.groupingBy(partition -> partition.getVault().getVaultId()));
			for (Map.Entry<String, List<VirtualPartition>> entry : partitionByVaultId.entrySet()) {
				String vaultId = entry.getKey();
				List<VirtualPartition> partitions = entry.getValue();
				Collections.sort(partitions, Comparator.comparingInt(VirtualPartition::getPartitionId));
				VaultPartitionInfo vaultPartitionInfo = devicePartitionInfo.getVaults().stream().filter(v -> v.getVaultId().equals(vaultId)).findFirst().orElse(null);
				if (vaultPartitionInfo == null) {
					vaultPartitionInfo = new VaultPartitionInfo().setVaultId(vaultId);
					devicePartitionInfo.addVaults(vaultPartitionInfo);
				}
				vaultPartitionInfo.setPartitionIds(partitions.stream().mapToInt(VirtualPartition::getPartitionId).toArray());
				vaultPartitionInfo.setPartitionSize(partitions.stream().mapToLong(VirtualPartition::getSize).toArray());
				vaultPartitionInfo.setPartitionFileCount(partitions.stream().mapToInt(VirtualPartition::getFileCount).toArray());
			}
		}

		if (clusterPartitionInfo.isActive()) {
			clusterFileSystemServiceClient.handlePartitionUpdate(clusterPartitionInfo);
		}

		return clusterPartitionInfo;
	}

}
