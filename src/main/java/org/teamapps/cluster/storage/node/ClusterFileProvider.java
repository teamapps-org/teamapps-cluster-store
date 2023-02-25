package org.teamapps.cluster.storage.node;

import org.teamapps.cluster.storage.VaultFile;

import java.io.IOException;
import java.io.InputStream;

public interface ClusterFileProvider {

	InputStream getClusterFile(String vaultId, VaultFile vaultFile, String password) throws IOException;
}
