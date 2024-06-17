package skiplisttodo

import (
	"fmt"
	"testing"
	"time"

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

func TestUpdate(t *testing.T) {
	skl := New(16)

	skl.Set([]byte("foo.1"), []byte("bar.1"))
	v, f := skl.Get([]byte("foo.1"))
	assert.True(t, f)
	assert.Equal(t, v, []byte("bar.1"))

	skl.Set([]byte("foo.1"), []byte("bar.22"))
	v, f = skl.Get([]byte("foo.1"))
	assert.True(t, f)
	assert.Equal(t, v, []byte("bar.22"))

}

func TestIter(t *testing.T) {
	skl := New(16)
	for i := 1; i < 100; i++ {
		skl.Set([]byte(fmt.Sprintf("foo.%d", i)), []byte(fmt.Sprintf("bar.%d", i)))
	}

	i := 1
	j := -1
	it := skl.NewIterator()
	for it.Next() {
		if j == -1 {
			k := []byte(fmt.Sprintf("foo.%d", i))
			v := []byte(fmt.Sprintf("bar.%d", i))

			assert.Equal(t, k, it.Key())
			assert.Equal(t, v, it.Value())

			j++
			continue
		}

		k := []byte(fmt.Sprintf("foo.%d%d", i, j))
		v := []byte(fmt.Sprintf("bar.%d%d", i, j))

		assert.Equal(t, k, it.Key())
		assert.Equal(t, v, it.Value())

		if j == 9 {
			j = -1
			i++
		} else {
			j++
		}
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

	t1 := time.Now()
	for i := 0; i < 100000; i++ {
		skl.Set([]byte(fmt.Sprintf("foo.%d", i)), []byte(fmt.Sprintf("bar.%d", i)))
	}

	fmt.Printf("Set took: %v\n", time.Since(t1))

	t2 := time.Now()
	for i := 0; i < b.N; i++ {
		for i := 0; i < 100000; i++ {
			_, _ = skl.Get([]byte(fmt.Sprintf("foo.%d", i)))
		}
	}
	fmt.Printf("Get took: %v\n", time.Since(t2))
}
