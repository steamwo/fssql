package kernel

import (
	_ "github.com/steamwo/fssql/internal/core/memory"
	_ "github.com/steamwo/fssql/internal/core/types"
	_ "github.com/steamwo/fssql/internal/core/vector"
	_ "github.com/steamwo/fssql/internal/execution/expression"
	_ "github.com/steamwo/fssql/internal/execution/hash"
	_ "github.com/steamwo/fssql/internal/execution/pipeline"
	_ "github.com/steamwo/fssql/internal/execution/scheduler"
	"testing"
)

func TestPhase0KernelPackagesCompile(t *testing.T) {}
