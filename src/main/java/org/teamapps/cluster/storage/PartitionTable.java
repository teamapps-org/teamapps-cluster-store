package org.teamapps.cluster.storage;

import org.teamapps.cluster.storage.node.calc.PhysicalStorage;
import org.teamapps.cluster.storage.node.calc.Vault;
import org.teamapps.cluster.storage.node.calc.VirtualPartition;

import java.util.ArrayList;
import java.util.List;

public class PartitionTable {

	private List<Vault> vaults = new ArrayList<>();
	private List<PhysicalStorage> storages = new ArrayList<>();

	public PartitionTable(List<Vault> vaults, List<PhysicalStorage> storages) {
		this.vaults = vaults;
		this.storages = storages;
	}

	public PartitionTable() {
	}

	public void addStorage(PhysicalStorage storage) {
		storages.add(storage);
	}

	public void addVault(Vault vault) {
		vaults.add(vault);
	}

	private long getCapacity() {
		return storages.stream()
				.mapToLong(PhysicalStorage::getCapacity)
				.sum();
	}

	private int getPartitions() {
		return vaults.stream()
				.mapToInt(vault -> (int) vault.getPartitionSetById().values().stream().mapToLong(set -> set.getReplicas().size()).sum())
				.sum();
	}

	public boolean calculatePartitions() {
		long capacity = getCapacity();
		int partitions = getPartitions();
		//long baseSize = capacity / partitions / 4;
		System.out.println("capacity:" + capacity + ", partitions:" + partitions);

		List<VirtualPartition> virtualPartitions = vaults.stream().flatMap(vault -> vault.getPartitionSetById().values().stream())
				.flatMap(partitionSet -> partitionSet.getReplicas().stream())
				.filter(virtualPartition -> virtualPartition.getPhysicalStorage() == null)
				.toList();

		for (VirtualPartition virtualPartition : virtualPartitions) {
//			System.out.println("Get best storage:");
			PhysicalStorage bestStorage = getBestStorage(virtualPartition);
			if (bestStorage == null) {
				return false;
			}
//			System.out.println("result:" + virtualPartition.getDistance(bestStorage) + ", " + bestStorage);
			virtualPartition.setPhysicalStorage(bestStorage);
			bestStorage.addVirtualPartition(virtualPartition);
		}

		for (PhysicalStorage storage : storages) {
			System.out.println(storage);
			storage.getVirtualPartitions().forEach(vp -> System.out.println("\t" + vp));
		}

		for (Vault vault : vaults) {
			System.out.println(vault.getVaultId() + ", min-distance:" + vault.getMinDistance() + ", base-distance:" + vault.getBaseDistance());
		}
		return true;
	}

	private PhysicalStorage getBestStorage(VirtualPartition virtualPartition) {
		long minPartitionSize = virtualPartition.getVault().getMinPartitionSize();
		PhysicalStorage bestStorage = null;
		int bestDistance = 0;
		for (PhysicalStorage storage : storages) {
//			System.out.println("\tstorage:" + storage.toString());
//			System.out.println(virtualPartition.getSize() + ", " + minPartitionSize + ", " + storage.getVirtualSpace() + ", " + storage.getCapacity());
			if (storage.isAvailable(virtualPartition.getSize() + minPartitionSize)) {
				int distance = virtualPartition.getDistance(storage);
//				System.out.println("\td:" + distance + ", vs1:" + (bestStorage == null ? "-" : bestStorage.getVirtualSpace()) + ">" + storage.getVirtualSpace());
				if (distance > 0) {
					if (distance > bestDistance) {
						bestStorage = storage;
						bestDistance = distance;
//						System.out.println("\tdistance:" + distance);
					} else if (distance == bestDistance && storage.getVirtualSpace() >= bestStorage.getVirtualSpace()) {
						bestStorage = storage;
						bestDistance = distance;
//						System.out.println("\tdistance:" + distance);
					}
				}
			}
		}
		return bestStorage;
	}


	public static void main(String[] args) {
		PartitionTable partitionTable = new PartitionTable();
		partitionTable.addStorage(new PhysicalStorage("disc-a", "node-1", "dc-1", "cc", 5_000));
		partitionTable.addStorage(new PhysicalStorage("disc-b", "node-1", "dc-1", "cc", 5_000));
		partitionTable.addStorage(new PhysicalStorage("disc-a", "node-2", "dc-1", "cc", 5_000));
		partitionTable.addStorage(new PhysicalStorage("disc-a", "node-3", "dc-2", "cc", 3_000));
		partitionTable.addStorage(new PhysicalStorage("disc-a", "node-4", "dc-3", "fi", 5_000));
		partitionTable.addStorage(new PhysicalStorage("disc-a", "node-5", "dc-4", "ch", 2_000));

		partitionTable.addVault(new Vault("vault-1", 3, 1000, 0));
		partitionTable.addVault(new Vault("vault-2", 3, 1000, 0));
		partitionTable.addVault(new Vault("vault-3", 3, 1000, 0));

		partitionTable.calculatePartitions();
	}
}
