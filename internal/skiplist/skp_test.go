package skiplist

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasic(t *testing.T) {
	skl := New(16)

	for i := 0; i < 1000000; i++ {
		skl.Set([]byte(fmt.Sprintf("foo.%d", i)), []byte(fmt.Sprintf("bar.%d", i)))
	}

	for i := 0; i < 1000000; i++ {
		v, f := skl.Get([]byte(fmt.Sprintf("foo.%d", i)))
		assert.True(t, f)
		assert.Equal(t, v, []byte(fmt.Sprintf("bar.%d", i)))
	}
}

func BenchmarkBasicSet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		skl := New(16)

		for i := 0; i < 100000; i++ {
			skl.Set([]byte(fmt.Sprintf("foo.%d", i)), []byte(fmt.Sprintf("bar.%d", i)))
		}
	}
}

func BenchmarkBasicGet(b *testing.B) {
	skl := New(16)

	for i := 0; i < 100000; i++ {
		skl.Set([]byte(fmt.Sprintf("foo.%d", i)), []byte(fmt.Sprintf("bar.%d", i)))
	}

	for i := 0; i < b.N; i++ {
		for i := 0; i < 100000; i++ {
			_, _ = skl.Get([]byte(fmt.Sprintf("foo.%d", i)))
		}
	}
}
