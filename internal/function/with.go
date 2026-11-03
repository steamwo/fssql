package function

import "fmt"

// With returns a catalog snapshot containing d without mutating the receiver.
// It is used by composition roots that freeze extension definitions before
// exposing the catalog to concurrent query binding.
func (c *Catalog) With(d Definition) (*Catalog, error) {
	if c == nil {
		return nil, fmt.Errorf("function catalog is nil")
	}
	if d.ID == InvalidID {
		return nil, fmt.Errorf("invalid function definition")
	}
	if _, exists := c.LookupID(d.ID); exists {
		return nil, fmt.Errorf("function id %d already registered", d.ID)
	}
	out := NewCatalog()
	c.mu.RLock()
	for name, definition := range c.byName {
		out.byName[name] = definition
	}
	c.mu.RUnlock()
	if err := out.Register(d); err != nil {
		return nil, err
	}
	return out, nil
}
