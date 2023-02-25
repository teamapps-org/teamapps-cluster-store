package org.teamapps.cluster.storage.node.calc;

import java.util.HashMap;
import java.util.Map;

public class Vault {

	public static final int VIRTUAL_PARTITIONS = 256;
	private final String vaultId;
	private int requiredCopies;
	private long minVaultSize;
	private int priority;
	private final Map<Integer, VirtualPartitionSet> partitionSetById = new HashMap<>();

	public Vault(String vaultId) {
		this.vaultId = vaultId;
	}

	public Vault(String vaultId, int requiredCopies, long minVaultSize, int priority) {
		this.vaultId = vaultId;
		this.minVaultSize = minVaultSize;
		this.priority = priority;
		createMissingPartitions(requiredCopies);
	}

	public void addPartition(int partitionId, long size, int fileCount, PhysicalStorage physicalStorage) {
		VirtualPartitionSet virtualPartitionSet = partitionSetById.computeIfAbsent(partitionId, id -> new VirtualPartitionSet(this, id));
		VirtualPartition partition = new VirtualPartition(virtualPartitionSet, size, fileCount);
		partition.setPhysicalStorage(physicalStorage);
		physicalStorage.addVirtualPartition(partition);
		virtualPartitionSet.addReplica(partition);
	}

	public void createMissingPartitions(int requiredCopies) {
		this.requiredCopies = requiredCopies;
		for (int partitionId = 0; partitionId < VIRTUAL_PARTITIONS; partitionId++) {
			VirtualPartitionSet virtualPartitionSet = partitionSetById.computeIfAbsent(partitionId, id -> new VirtualPartitionSet(this, id));
			while (virtualPartitionSet.getReplicas().size() < requiredCopies) {
				virtualPartitionSet.addReplica(new VirtualPartition(virtualPartitionSet, 0, 0));
			}
		}
		partitionSetById.values().forEach(VirtualPartitionSet::setReplicaIndices);
	}

	public int getMinDistance() {
		int minDistance = Integer.MAX_VALUE;
		for (VirtualPartitionSet partitionSet : partitionSetById.values()) {
			int distance = partitionSet.getMinDistance();
			if (distance < minDistance) {
				minDistance = distance;
			}
		}
		return minDistance;
	}

	public int getBaseDistance() {
		int minDistance = Integer.MAX_VALUE;
		for (VirtualPartitionSet partitionSet : partitionSetById.values()) {
			int distance = partitionSet.getMaxDistance();
			if (distance < minDistance) {
				minDistance = distance;
			}
		}
		return minDistance;
	}

	public long getMinPartitionSize() {
		return minVaultSize / VIRTUAL_PARTITIONS;
	}

	public void setRequiredCopies(int requiredCopies) {
		this.requiredCopies = requiredCopies;
	}

	public long getMinVaultSize() {
		return minVaultSize;
	}

	public void setMinVaultSize(long minVaultSize) {
		this.minVaultSize = minVaultSize;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	public String getVaultId() {
		return vaultId;
	}

	public int getRequiredCopies() {
		return requiredCopies;
	}


	public int getPriority() {
		return priority;
	}

	public Map<Integer, VirtualPartitionSet> getPartitionSetById() {
		return partitionSetById;
	}
}

