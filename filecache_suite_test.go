package filecache

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestFilecache(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Filecache Suite")
}
