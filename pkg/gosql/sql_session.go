package gosql

func newSQLConnection(parent *DB) (*sqlConn, error) {
	engine, err := parent.engine.ForkSession()
	if err != nil {
		return nil, err
	}
	local := &DB{owner: parent, memory: parent.memory, buffers: parent.buffers, temporary: parent.temporary, engine: engine, materialized: parent.materialized, spillMemoryBudget: parent.spillMemoryBudget}
	return &sqlConn{db: local}, nil
}
