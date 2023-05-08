package server

import "path/filepath"

func GetFullPath(tabletName string) string {
	prefix := "../../files"
	return filepath.Join(prefix, tabletName)
}
