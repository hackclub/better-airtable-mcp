package approval

import (
	"context"
	"embed"
	"io/fs"

	"github.com/hackclub/better-airtable-mcp/internal/logx"
)

//go:embed dist
var embeddedAssets embed.FS

func approvalAssetFS() fs.FS {
	assets, err := fs.Sub(embeddedAssets, "dist")
	if err != nil {
		logx.Event(context.Background(), "approval_assets", "approval.assets_missing",
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
			"fatal", true,
		)
		panic("approval assets are not embedded correctly: " + err.Error())
	}
	return assets
}
