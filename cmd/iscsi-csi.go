/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"

	"github.com/zdnscloud/cement/log"
	"github.com/zdnscloud/csi-iscsi-plugin/pkg/iscsi"
	"github.com/zdnscloud/gok8s/client"
	"github.com/zdnscloud/gok8s/client/config"
)

func init() {
	flag.Set("logtostderr", "true")
}

var (
	endpoint   = flag.String("endpoint", "unix://tmp/csi.sock", "CSI endpoint")
	driverName = flag.String("drivername", "k8s-csi-iscsi", "name of the driver")
	nodeID     = flag.String("nodeid", "", "node id")
	vgName     = flag.String("vgname", "iscsi-group", "volume group name")
	poolName   = flag.String("poolname", "iscsi-pool", "volume pool name")
	labelKey   = flag.String("labelKey", "storage.zcloud.cn/storagetype", "storage host label key")
	labelValue = flag.String("labelValue", "", "storage host label value")
	lvmdDsName = flag.String("lvmdDsName", "", "lvmd daemonset name")
)

func main() {
	flag.Parse()

	log.InitLogger(log.Debug)
	config, err := config.GetConfig()
	if err != nil {
		log.Fatalf("get k8s config failed:%s", err.Error())
	}

	cli, err := client.New(config, client.Options{})
	if err != nil {
		log.Fatalf("create k8s client failed:%s", err.Error())
	}
	conf := &iscsi.PluginConf{
		Endpoint:   *endpoint,
		DriverName: *driverName,
		NodeID:     *nodeID,
		VgName:     *vgName,
		PoolName:   *poolName,
		LabelKey:   *labelKey,
		LabelValue: *labelValue,
		LvmdDsName: *lvmdDsName,
	}

	driver := iscsi.NewDriver(cli)
	driver.Run(conf)
}
