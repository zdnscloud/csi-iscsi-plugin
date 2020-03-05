package iscsi

type PluginConf struct {
	Endpoint    string
	DriverName  string
	NodeID      string
	VgName      string
	PoolName    string
	LabelKey    string
	LabelValue  string
	LvmdDsName string
}
