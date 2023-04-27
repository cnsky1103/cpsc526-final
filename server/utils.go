package server

import "path/filepath"

func GetFullPath(tabletName string) string {
	prefix := "/Users/ybyan/Documents/GitHub/cpsc526-final/files"
	return filepath.Join(prefix, tabletName)
}
