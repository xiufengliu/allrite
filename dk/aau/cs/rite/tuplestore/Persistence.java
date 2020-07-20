package dk.aau.cs.rite.tuplestore;

import dk.aau.cs.rite.common.RiteException;

public interface Persistence {
	void backup(String path) throws RiteException;
	void recover();
}
