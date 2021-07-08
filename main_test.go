package search_light

import (
	"testing"

	"github.com/elissonalvesilva/search-light/indexador"
)

func BenchmarkIndex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		indexador.Index()
	}
}
