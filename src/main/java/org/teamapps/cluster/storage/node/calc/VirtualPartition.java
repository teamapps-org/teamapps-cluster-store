package org.teamapps.cluster.storage.node.calc;

import java.util.ArrayList;
import java.util.List;

public class VirtualPartition {

	private final Vault vault;
	private final VirtualPartitionSet partitionSet;
	private final int partitionId;
	private final long size;
	private final int fileCount;
	private int index;
	private PhysicalStorage physicalStorage;

	public VirtualPartition(VirtualPartitionSet partitionSet, long size, int fileCount) {
		this.partitionSet = partitionSet;
		this.vault = partitionSet.getVault();
		this.partitionId = partitionSet.getPartitionId();
		this.size = size;
		this.fileCount = fileCount;
	}

	public int getDistance(PhysicalStorage storage) {
		return partitionSet.getDistance(storage);
	}

	public VirtualPartitionSet getPartitionSet() {
		return partitionSet;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public Vault getVault() {
		return vault;
	}

	public int getPartitionId() {
		return partitionId;
	}

	public int getIndex() {
		return index;
	}

	public long getSize() {
		return size;
	}

	public PhysicalStorage getPhysicalStorage() {
		return physicalStorage;
	}

	public void setPhysicalStorage(PhysicalStorage physicalStorage) {
		this.physicalStorage = physicalStorage;
	}

	@Override
	public String toString() {
		return vault.getVaultId() + ": " + partitionId + " (" + index + ")";
	}

	public int getFileCount() {
		return fileCount;
	}
}
