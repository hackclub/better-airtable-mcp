package approval

import (
	"embed"
	"io/fs"
	"log"
)

//go:embed dist
var embeddedAssets embed.FS

func approvalAssetFS() fs.FS {
	assets, err := fs.Sub(embeddedAssets, "dist")
	if err != nil {
		log.Panicf("approval assets are not embedded correctly: %v", err)
	}
	return assets
}
