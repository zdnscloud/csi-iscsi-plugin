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
	"time"

	"github.com/zdnscloud/gok8s/client"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/zdnscloud/cement/log"
	"github.com/zdnscloud/csi-iscsi-plugin/pkg/csi-common"
	pb "github.com/zdnscloud/lvmd/proto"
)

const (
	DefaultFS      = "ext4"
	ConnectTimeout = 3 * time.Second
	Gigabytes      = int64(1024 * 1024 * 1024)
)

type controllerServer struct {
	*csicommon.DefaultControllerServer
	client client.Client
	conf   *PluginConf
}

func NewControllerServer(d *csicommon.CSIDriver, c client.Client, conf *PluginConf) *controllerServer {
	return &controllerServer{
		DefaultControllerServer: csicommon.NewDefaultControllerServer(d),
		client:                  c,
		conf:                    conf,
	}
}

func (cs *controllerServer) GetNodeID() string {
	return cs.conf.NodeID
}

func (cs *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if err := cs.Driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		log.Errorf("invalid create volume req: %v", req)
		return nil, err
	}

	if len(req.Name) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume Name cannot be empty")
	}
	if req.VolumeCapabilities == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities cannot be empty")
	} else {
		for _, cap := range req.VolumeCapabilities {
			if cap.GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER {
				return nil, status.Error(codes.InvalidArgument, "only single node writer is supported")
			}
		}
	}

	volumeId := req.GetName()
	requireBytes := req.GetCapacityRange().GetRequiredBytes()
	allocateBytes := useGigaUnit(requireBytes)

	devicePath := filepath.Join("/dev/", cs.conf.VgName, volumeId)

	if _, err := os.Stat(devicePath); os.IsNotExist(err) {
		if err := cs.createVolume(ctx, volumeId, allocateBytes); err != nil {
			return nil, err
		}
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeId,
			CapacityBytes: allocateBytes,
			VolumeContext: req.GetParameters(),
			AccessibleTopology: []*csi.Topology{
				{
					Segments: map[string]string{
						cs.conf.LabelKey: cs.conf.LabelValue,
					},
				},
			},
		},
	}, nil
}

//only support ReadWriteOnce
func (cs *controllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	for _, cap := range req.VolumeCapabilities {
		if cap.GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER {
			return &csi.ValidateVolumeCapabilitiesResponse{}, nil
		}
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.VolumeCapabilities,
		},
	}, nil
}

func (cs *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	volumeId := req.GetVolumeId()
	return &csi.DeleteVolumeResponse{}, cs.deleteVolume(ctx, volumeId)
}

func (cs *controllerServer) validateExpandVolumeRequest(req *csi.ControllerExpandVolumeRequest) error {
	if err := cs.Driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_EXPAND_VOLUME); err != nil {
		return fmt.Errorf("invalid ExpandVolumeRequest: %v", err)
	}

	if req.GetVolumeId() == "" {
		return status.Error(codes.InvalidArgument, "Volume ID cannot be empty")
	}

	capRange := req.GetCapacityRange()
	if capRange == nil {
		return status.Error(codes.InvalidArgument, "CapacityRange cannot be empty")
	}

	return nil
}

func (cs *controllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	if err := cs.validateExpandVolumeRequest(req); err != nil {
		return nil, fmt.Errorf("ControllerExpandVolumeRequest validation failed: %v", err)
	}
	volumeId := req.GetVolumeId()
	requireBytes := req.GetCapacityRange().GetRequiredBytes()
	allocateBytes := useGigaUnit(requireBytes)

	pv, err := getPV(cs.client, volumeId)
	if err != nil {
		return nil, fmt.Errorf("Failed to get pv by volumeId %s: %v", volumeId, err)
	}

	attach, err := isAttached(cs.client, pv.Name)
	if err != nil {
		return nil, fmt.Errorf("Failed to get volumeattachments for pv %s: %v", pv.Name, err)
	}
	if attach {
		return nil, fmt.Errorf("pv %s is attaching now, can not expand", pv.Name)
	}

	if err := cs.resizeVolume(ctx, volumeId, allocateBytes); err != nil {
		return nil, err
	}

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         allocateBytes,
		NodeExpansionRequired: false,
	}, nil
}

func (cs *controllerServer) resizeVolume(ctx context.Context, volumeId string, size int64) error {

	conn, err := createLvmdClient(cs.client, cs.GetNodeID(), cs.conf.LvmdDsName, false)
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("Failed to connect to lvmd on %v: %v", cs.GetNodeID(), err))
	}
	defer conn.Close()

	if err := checkVolumeExist(conn, cs.conf.VgName, volumeId); err != nil {
		return status.Errorf(codes.Internal, "Error in ListLogicalVolume: err=%v", err)
	}

	_, err = conn.ChangeLV(ctx, &pb.ChangeLVRequest{
		VolumeGroup: cs.conf.VgName,
		Name:        volumeId,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "Error in ChangeLogicalVolume: err=%v", err)
	}
	resp, err := conn.ResizeLV(ctx, &pb.ResizeLVRequest{
		VolumeGroup: cs.conf.VgName,
		Name:        volumeId,
		Size:        uint64(size),
	})
	log.Infof("ResizeLV: %v", resp)
	if err != nil {
		return status.Errorf(codes.Internal, "Error in ResizeLogicalVolume: err=%v", err)
	}
	return nil
}

func (cs *controllerServer) createVolume(ctx context.Context, volumeId string, size int64) error {
	conn, err := createLvmdClient(cs.client, cs.GetNodeID(), cs.conf.LvmdDsName, false)
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("Failed to connect to lvmd on %v: %v", cs.GetNodeID(), err))
	}
	defer conn.Close()

	resp, err := conn.CreateLV(ctx, &pb.CreateLVRequest{
		VolumeGroup: cs.conf.VgName,
		Name:        volumeId,
		Size:        uint64(size),
	})
	log.Infof("CreateLV: %v", resp)
	if err != nil {
		return status.Errorf(codes.Internal, "Error in CreateLogicalVolume: err=%v", err)
	}
	return nil
}

func (cs *controllerServer) deleteVolume(ctx context.Context, volumeId string) error {
	conn, err := createLvmdClient(cs.client, cs.GetNodeID(), cs.conf.LvmdDsName, false)
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("Failed to connect to lvmd on %v: %v", cs.GetNodeID(), err))
	}
	defer conn.Close()

	if err := checkVolumeExist(conn, cs.conf.VgName, volumeId); err != nil {
		return status.Errorf(codes.Internal, "Error in ListLogicalVolume: err=%v", err)
	}
	resp, err := conn.RemoveLV(ctx, &pb.RemoveLVRequest{
		VolumeGroup: cs.conf.VgName,
		Name:        volumeId,
	})
	log.Infof("RemoveLV: %v", resp)
	if err != nil {
		return status.Errorf(codes.Internal, "Error in DeleteLogicalVolume: err=%v", err)
	}
	return nil
}
