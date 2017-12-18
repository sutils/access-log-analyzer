package main

import (
	"fmt"
	"testing"

	"github.com/Centny/gwf/util"
)

func TestIp(t *testing.T) {
	ip := "14.23.162.173"
	script := fmt.Sprintf(`/usr/bin/php -r "require 'IP.class.php'; print implode(',',IP::find('%v'));"`, ip)
	fmt.Println(util.Exec2(script))
}
