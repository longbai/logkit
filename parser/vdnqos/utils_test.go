package vdnqos

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestIsAndroidDevice(t *testing.T) {
	s := "104153993"
	isAndroidDevice := IsAndroidDevice(s)
	assert.EqualValues(t, false, isAndroidDevice)

	s = "156786086438882"
	isAndroidDevice = IsAndroidDevice(s)
	assert.EqualValues(t, true, isAndroidDevice)

	s = "E0B8E67C-57BD-462C-BDAF-0264227282C7"
	isAndroidDevice = IsAndroidDevice(s)
	assert.EqualValues(t, false, isAndroidDevice)

}