package main

import (
	"fmt"
	"os"
	"sync"

	"github.com/Centny/gwf/log"

	"github.com/Centny/gwf/routing"
	"github.com/Centny/gwf/util"
	"github.com/Centny/rediscache"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: %v <listen address> <redis server uri>\n", os.Args[0])
		return
	}
	rediscache.InitRedisPool(os.Args[2])
	log.I("start server on %v", os.Args[1])
	routing.HFunc("^/getIpInfo(\\?.*)?$", IpInfoH)
	routing.Shared.ShowSlow = 0
	routing.ListenAndServe(os.Args[1])
}

var Cache = rediscache.NewCache(1024 * 1024 * 300)
var reqLck = sync.RWMutex{}
var reqLast int64

func IpInfoH(hs *routing.HTTPSession) routing.HResult {
	ip := hs.CheckVal("ip")
	if len(ip) < 1 {
		return hs.MsgResE2(10, "arg-err", "ip is required")
	}
	beg := util.Now()
	var info = util.Map{}
	err := Cache.WillQuery(ip, &info, func() (val interface{}, err error) {
		reqLck.Lock()
		defer reqLck.Unlock()
		var res = util.Map{}
		script := fmt.Sprintf(`/usr/bin/php -r "require 'IP.class.php'; print implode(',',IP::find('%v'));"`, ip)
		res["loc"], err = util.Exec2(script)
		val = res
		return
	})
	if err == nil {
		log.D("get ip info success by %v, used: %vms", ip, util.Now()-beg)
		return hs.JRes(info)
	}
	log.E("get ip info by %v fail with %v", ip, err)
	return hs.MsgResErr2(20, "arg-err", err)
}
