package binder

import (
	"fmt"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/semantics"
	"strings"
)

func validateConstantRegex(id function.ID, args []*BoundExpression) error {
	if id != function.RegexpMatchesID && id != function.RegexpFullMatchID && id != function.RegexpReplaceID && id != function.RegexpExtractID && id != function.RegexpExtractAllID && id != function.RegexpSplitID {
		return nil
	}
	pattern, ok := constantString(args[1])
	if !ok {
		return nil
	}
	options := ""
	slot := -1
	if id == function.RegexpReplaceID {
		if len(args) == 4 {
			slot = 3
		}
	} else if id == function.RegexpExtractID || id == function.RegexpExtractAllID {
		if len(args) == 4 {
			slot = 3
		} else if len(args) == 3 && args[2].Type.ID == types.Varchar {
			slot = 2
		}
	} else if len(args) == 3 {
		slot = 2
	}
	if slot >= 0 {
		var ok bool
		options, ok = constantString(args[slot])
		if !ok {
			return fmt.Errorf("regular expression options must be constant")
		}
	}
	if id != function.RegexpReplaceID && strings.ContainsRune(options, 'g') {
		return fmt.Errorf("global option only applies to regexp_replace")
	}
	_, err := semantics.CompileRegex(pattern, options, id == function.RegexpFullMatchID)
	return err
}
