package org.teamapps.cluster.storage.node.calc;

import java.util.ArrayList;
import java.util.List;

public class VirtualPartitionSet {

	private final Vault vault;
	private final int partitionId;
	private final List<VirtualPartition> replicas = new ArrayList<>();

	public VirtualPartitionSet(Vault vault, int partitionId) {
		this.vault = vault;
		this.partitionId = partitionId;
	}

	public void addReplica(VirtualPartition partition) {
		replicas.add(partition);
	}

	public void setReplicaIndices() {
		for (int i = 0; i < replicas.size(); i++) {
			replicas.get(i).setIndex(i);
		}
	}

	public int getDistance(PhysicalStorage storage) {
		return replicas.stream()
				.mapToInt(partition -> partition.getPhysicalStorage() == null ? PhysicalStorage.getMaxDistance() : partition.getPhysicalStorage().getDistance(storage))
				.sum();
	}

	public int getMinDistance() {
		int minDistance = PhysicalStorage.getMaxDistance();
		for (VirtualPartition vp1 : replicas) {
			for (VirtualPartition vp2 : replicas) {
				if (vp1 != vp2) {
					int distance = vp1.getPhysicalStorage().getDistance(vp2.getPhysicalStorage());
					if (distance < minDistance) {
						minDistance = distance;
					}
				}
			}
		}
		return minDistance;
	}

	public int getMaxDistance() {
		int maxDistance = 0;
		for (VirtualPartition vp1 : replicas) {
			for (VirtualPartition vp2 : replicas) {
				if (vp1 != vp2) {
					int distance = vp1.getPhysicalStorage().getDistance(vp2.getPhysicalStorage());
					if (distance > maxDistance) {
						maxDistance = distance;
					}
				}
			}
		}
		return maxDistance;
	}

	public Vault getVault() {
		return vault;
	}

	public int getPartitionId() {
		return partitionId;
	}

	public List<VirtualPartition> getReplicas() {
		return replicas;
	}
}
