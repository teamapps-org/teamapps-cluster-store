import org.teamapps.cluster.message.protocol.ClusterNodeSystemInfo;
import org.teamapps.message.protocol.message.MessageDefinition;
import org.teamapps.message.protocol.message.MessageModelCollection;
import org.teamapps.message.protocol.model.ModelCollection;
import org.teamapps.message.protocol.model.ModelCollectionProvider;
import org.teamapps.message.protocol.service.ServiceProtocol;

public class MessageProtocol implements ModelCollectionProvider {
	@Override
	public ModelCollection getModelCollection() {
		MessageModelCollection modelCollection = new MessageModelCollection("clusterStoreProtocol", "org.teamapps.cluster.store.protocol", 1);

		MessageDefinition clusterFile = modelCollection.createModel("clusterFile", "cfs.file", false);
		MessageDefinition clusterFileDescriptor = modelCollection.createModel("clusterFileDescriptor", "cfs.fileDescriptor", false);

		MessageDefinition storageNodeConfig = modelCollection.createModel("storageNodeConfig", "cfs.storageNodeConfig");
		MessageDefinition storageDeviceConfig = modelCollection.createModel("storageDeviceConfig", "cfs.storageDeviceConfig");
		MessageDefinition vaultConfig = modelCollection.createModel("vaultConfig", "cfs.vaultConfig");

		MessageDefinition clusterPartitionInfo = modelCollection.createModel("clusterPartitionInfo", "cfs.clusterPartitionInfo");
		MessageDefinition nodePartitionInfo = modelCollection.createModel("nodePartitionInfo", "cfs.nodePartitionInfo");
		MessageDefinition storageDevicePartitionInfo = modelCollection.createModel("storageDevicePartitionInfo", "cfs.storageDevicePartitionInfo");
		MessageDefinition vaultPartitionInfo = modelCollection.createModel("vaultPartitionInfo", "cfs.vaultPartitionInfo");

		ClusterNodeSystemInfo systemInfo = new ClusterNodeSystemInfo();
		systemInfo.setMemorySize(345);
		systemInfo.setCores(3);
		MessageDefinition storageCacheConfig = modelCollection.createModel("storageCacheConfig", "cfs.storageCacheConfig", systemInfo, true);


		clusterPartitionInfo.addString("leaderNodeId", 1);
		clusterPartitionInfo.addMultiReference("nodes", nodePartitionInfo, 2);
		clusterPartitionInfo.addBoolean("active",3);

		nodePartitionInfo.addSingleReference("nodeConfig", storageNodeConfig, 1);
		nodePartitionInfo.addMultiReference("devices", storageDevicePartitionInfo, 2);

		storageDevicePartitionInfo.addSingleReference("deviceConfig", storageDeviceConfig, 1);
		storageDevicePartitionInfo.addMultiReference("vaults", vaultPartitionInfo, 2);

		vaultPartitionInfo.addString("vaultId", 1);
		vaultPartitionInfo.addIntArray("partitionIds", 2);
		vaultPartitionInfo.addIntArray("partitionFileCount", 3);
		vaultPartitionInfo.addLongArray("partitionSize", 4);


		storageNodeConfig.addString("nodeId", 1);
		storageNodeConfig.addString("dataCenterId", 2);
		storageNodeConfig.addString("countryCode", 3);
		storageNodeConfig.addMultiReference("storageDevices", storageDeviceConfig, 4);

		storageDeviceConfig.addString("deviceId", 1);
		storageDeviceConfig.addLong("capacity", 2);
		storageDeviceConfig.addString("localPath", 3);

		storageCacheConfig.addString("cachePath", 1);
		storageCacheConfig.addLong("maxCacheSize", 2);




		vaultConfig.addString("vaultId", 1);
		vaultConfig.addLong("minVaultSize", 2);
		vaultConfig.addInteger("requiredFileCopies", 3);
		vaultConfig.addInteger("priority", 4);

		clusterFile.addString("vaultId", 1);
		clusterFile.addString("descriptor", 2);
		clusterFile.addFile("file", 3);

		clusterFileDescriptor.addString("vaultId", 1);
		clusterFileDescriptor.addString("descriptor", 2);

		ServiceProtocol storageLeader = modelCollection.createService("clusterStorageLeader");
		storageLeader.addMethod("updateStorageNode", nodePartitionInfo, clusterPartitionInfo);
		storageLeader.addMethod("updateVault", vaultConfig, clusterPartitionInfo);


		ServiceProtocol fileSystemService = modelCollection.createService("clusterFileSystemService");
		fileSystemService.addMethod("retrieveClusterFile", clusterFileDescriptor, clusterFile);
		fileSystemService.addMethod("storeClusterFile", clusterFile, clusterFileDescriptor);
		fileSystemService.addMethod("checkClusterFileIntegrity", clusterFileDescriptor, clusterFileDescriptor);
		fileSystemService.addBroadcastMethod("handlePartitionUpdate", clusterPartitionInfo);

		return modelCollection;
	}
}



