package resolver

import (
	"github.com/zeromicro/go-zero/zrpc/resolver/internal"
)

// 把这个单独放在一个包中，可以使第三方手动注册

// Register registers schemes defined zrpc.
// Keep it in a separated package to let third party register manually.
func Register() {
	internal.RegisterResolver()
}
