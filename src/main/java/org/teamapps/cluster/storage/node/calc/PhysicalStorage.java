package org.teamapps.cluster.storage.node.calc;

import java.util.ArrayList;
import java.util.List;

public class PhysicalStorage {

	private final String discId;
	private final String nodeId;
	private final String dataCenterId;
	private final String countryCode;
	private final long capacity;
	private long usedCapacity;
	private final List<VirtualPartition> virtualPartitions = new ArrayList<>();

	public static int getMaxDistance() {
		return 10_000;
	}

	public PhysicalStorage(String discId, String nodeId, String dataCenterId, String countryCode, long capacity) {
		this.discId = discId;
		this.nodeId = nodeId;
		this.dataCenterId = dataCenterId;
		this.countryCode = countryCode;
		this.capacity = capacity;
	}

	public int getDistance(PhysicalStorage physicalStorage) {
		if (discId.equals(physicalStorage.getDiscId()) && nodeId.equals(physicalStorage.getNodeId())) {
			return 0;
		} else if (nodeId.equals(physicalStorage.getNodeId())) {
			return 10;
		} else if (dataCenterId.equals(physicalStorage.getDataCenterId())) {
			return 100;
		} else if (countryCode.equals(physicalStorage.getCountryCode())) {
			return 1_000;
		} else {
			return 10_000;
		}
	}

	public void addVirtualPartition(VirtualPartition partition) {
		virtualPartitions.add(partition);
		usedCapacity += partition.getSize();
	}

	public List<VirtualPartition> getVirtualPartitions() {
		return virtualPartitions;
	}

	public String getDiscId() {
		return discId;
	}

	public String getNodeId() {
		return nodeId;
	}

	public String getDataCenterId() {
		return dataCenterId;
	}

	public String getCountryCode() {
		return countryCode;
	}

	public long getCapacity() {
		return capacity;
	}

	public long getVirtualSpace() {
		return capacity - usedCapacity - virtualPartitions.stream().mapToLong(vp -> vp.getVault().getMinPartitionSize()).sum();
	}

	public boolean isAvailable(long additionalUsage) {
		return getVirtualSpace() - additionalUsage > 0;
	}

	@Override
	public String toString() {
		return "PhysicalStorage{" +
				"discId='" + discId + '\'' +
				", nodeId='" + nodeId + '\'' +
				", dataCenterId='" + dataCenterId + '\'' +
				", countryCode='" + countryCode + '\'' +
				", capacity=" + capacity +
				" virtual-space=" + getVirtualSpace() +
				'}';
	}
}
