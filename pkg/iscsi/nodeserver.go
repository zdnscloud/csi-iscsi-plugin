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

package iscsi

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/zdnscloud/gok8s/client"
	"golang.org/x/net/context"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/volume/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/zdnscloud/cement/log"
	"github.com/zdnscloud/csi-iscsi-plugin/pkg/csi-common"
	pb "github.com/zdnscloud/lvmd/proto"
)

type nodeServer struct {
	*csicommon.DefaultNodeServer
	client client.Client
	conf   *PluginConf
}

func NewNodeServer(d *csicommon.CSIDriver, c client.Client, conf *PluginConf) *nodeServer {
	return &nodeServer{
		DefaultNodeServer: csicommon.NewDefaultNodeServer(d),
		client:            c,
		conf:              conf,
	}
}

func (ns *nodeServer) GetNodeID() string {
	return ns.conf.NodeID
}

func (ns *nodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	volumeId := req.GetVolumeId()
	devicePath := filepath.Join("/dev/", ns.conf.VgName, volumeId)

	if _, err := os.Stat(devicePath); os.IsNotExist(err) {
		if err := ns.changeVolume(ctx, volumeId); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	notMnt, err := mount.New("").IsLikelyNotMountPoint(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(targetPath, 0750); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			notMnt = true
		} else {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	log.Debugf("Determining filesystem type at %v", devicePath)
	existingFstype, err := determineFilesystemType(devicePath)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Cannot determine filesystem type: err=%v",
			err)
	}
	log.Debugf("Existing filesystem type is '%v'", existingFstype)
	if existingFstype == "" {
		// There is no existing filesystem on the
		// device, format it with the requested
		// filesystem.
		log.Debugf("The device %v has no existing filesystem, formatting with %v", devicePath, DefaultFS)
		if err := formatDevice(devicePath, DefaultFS); err != nil {
			return nil, status.Errorf(
				codes.Internal,
				"formatDevice failed: err=%v",
				err)
		}
		existingFstype = DefaultFS
	}

	// Volume Mount
	if notMnt {
		// Get Options
		var options []string
		if req.GetReadonly() {
			options = append(options, "ro")
		} else {
			options = append(options, "rw")
		}
		mountFlags := req.GetVolumeCapability().GetMount().GetMountFlags()
		options = append(options, mountFlags...)

		// Mount
		mounter := mount.New("")
		err = mounter.Mount(devicePath, targetPath, DefaultFS, options)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	notMnt, err := mount.New("").IsLikelyNotMountPoint(targetPath)

	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Error(codes.NotFound, "Targetpath not found")
		} else {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	if notMnt {
		return nil, status.Error(codes.NotFound, "Volume not mounted")
	}

	log.Infof("UnmountPath: %s", targetPath)
	err = util.UnmountPath(targetPath, mount.New(""))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	parentDir := targetPath[:strings.LastIndex(targetPath, "/")]
	log.Infof("Remove CSI volume path: %s", parentDir)
	if err := os.RemoveAll(parentDir); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (ns *nodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	return &csi.NodeStageVolumeResponse{}, nil
}

func (ns *nodeServer) changeVolume(ctx context.Context, volumeId string) error {
	conn, err := createLvmdClient(ns.client, ns.GetNodeID(), ns.conf.LvmdDsName, true)
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("Failed to connect to lvmd on %v: %v", ns.GetNodeID(), err))
	}
	defer conn.Close()

	if err := checkVolumeExist(conn, ns.conf.VgName, volumeId); err != nil {
		return status.Errorf(codes.Internal, "Error in ListLogicalVolume: err=%v", err)
	}

	resp, err := conn.ChangeLV(ctx, &pb.ChangeLVRequest{
		VolumeGroup: ns.conf.VgName,
		Name:        volumeId,
	})
	log.Infof("ChangeLV: %v", resp)
	if err != nil {
		return status.Errorf(codes.Internal, "Error in ChangeLogicalVolume: err=%v", err)
	}
	return nil
}
