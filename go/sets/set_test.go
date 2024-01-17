package sets

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSet(t *testing.T) {
	testSet := New[int](1, 2, 3)

	assert.Equal(t, testSet.Len(), 3)

	testSet.Insert(4, 5, 6)
	compareSet := New[int](1, 2, 3, 4, 5, 6)
	assert.Equal(t, testSet, compareSet)

	assert.True(t, testSet.Equal(compareSet))
	compareSet.Insert(-1, -2)
	assert.False(t, testSet.Equal(compareSet))

	//tests for Difference func
	diffSet := New[int](-1, -2)
	assert.Equal(t, diffSet, compareSet.Difference(testSet))

	//tests for Has func
	assert.True(t, testSet.Has(3))
	assert.False(t, testSet.Has(-1))

	//tests for HasAny func
	assert.True(t, testSet.HasAny(1, 10, 11, 12))
	assert.False(t, testSet.HasAny(-1, 10, 11, 12))

	//tests for Delete func
	testSet.Delete(1, 2, 3, 4, 5, 6)
	assert.Empty(t, testSet)

}
