package function

// LookupID 按稳定内部 ID 查找函数定义；FunctionCatalog 规模很小，直接在受锁定义表中扫描，避免维护第二份可漂移索引。
func (c *Catalog) LookupID(id ID) (Definition, bool) {
	if c == nil || id == InvalidID {
		return Definition{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, definition := range c.byName {
		if definition.ID == id {
			return definition, true
		}
	}
	return Definition{}, false
}
