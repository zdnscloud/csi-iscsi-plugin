package iscsi

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os/exec"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/kubelet/apis"

	"github.com/zdnscloud/gok8s/client"
	lvmdclient "github.com/zdnscloud/lvmd/client"
	pb "github.com/zdnscloud/lvmd/proto"
)

const (
	NodeLabelKey     = apis.LabelHostname
	lvmdPort         = "1736"
	StorageNamespace = "zcloud"
)

func getLVMDAddr(cli client.Client, node, ds string, local bool) (string, error) {
	selector, err := getSelector(cli, ds)
	if err != nil {
		return "", fmt.Errorf("get selector for daemonSet %s failed, %v", ds, err)
	}

	pods, err := getPods(cli, selector)
	if err != nil {
		return "", fmt.Errorf("list pods which have selector %s failed, %v", selector, err)
	}
	if local == false {
		rand.Seed(time.Now().Unix())
		return pods.Items[rand.Int()%len(pods.Items)].Status.PodIP + ":" + lvmdPort, nil
	}
	for _, pod := range pods.Items {
		if pod.Spec.NodeName == node {
			return pod.Status.PodIP + ":" + lvmdPort, nil
		}
	}
	return "", errors.New(fmt.Sprintf("can not find lvmd on node %s", node))
}

func getPods(cli client.Client, selector labels.Selector) (*corev1.PodList, error) {
	pods := &corev1.PodList{}
	if err := cli.List(context.TODO(), &client.ListOptions{Namespace: StorageNamespace, LabelSelector: selector}, pods); err != nil {
		return nil, err
	}
	return pods, nil
}

func getSelector(cli client.Client, name string) (labels.Selector, error) {
	daemonSet := &appsv1.DaemonSet{}
	if err := cli.Get(context.TODO(), k8stypes.NamespacedName{StorageNamespace, name}, daemonSet); err != nil {
		return nil, err
	}
	return metav1.LabelSelectorAsSelector(daemonSet.Spec.Selector)
}

func getPV(client client.Client, volumeId string) (*corev1.PersistentVolume, error) {
	var pv corev1.PersistentVolume
	err := client.Get(context.TODO(), k8stypes.NamespacedName{"", volumeId}, &pv)
	if err != nil {
		return nil, err
	} else {
		return &pv, nil
	}
}

func getNode(client client.Client, nodeId string) (*corev1.Node, error) {
	var node corev1.Node
	err := client.Get(context.TODO(), k8stypes.NamespacedName{"", nodeId}, &node)
	if err != nil {
		return nil, err
	} else {
		return &node, nil
	}
}

func formatDevice(devicePath, fstype string) error {
	output, err := exec.Command("mkfs", "-t", fstype, devicePath).CombinedOutput()
	if err != nil {
		return errors.New("csi-lvm: formatDevice: " + string(output))
	}
	return nil
}

func determineFilesystemType(devicePath string) (string, error) {
	// We use `file -bsL` to determine whether any filesystem type is detected.
	// If a filesystem is detected (ie., the output is not "data", we use
	// `blkid` to determine what the filesystem is. We use `blkid` as `file`
	// has inconvenient output.
	// We do *not* use `lsblk` as that requires udev to be up-to-date which
	// is often not the case when a device is erased using `dd`.
	output, err := exec.Command("lsblk", "-o", "FSTYPE", "--noheadings", devicePath).CombinedOutput()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil

	output, err = exec.Command("file", "-bsL", devicePath).CombinedOutput()
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(string(output)) == "data" {
		// No filesystem detected.
		return "", nil
	}
	// Some filesystem was detected, we use blkid to figure out what it is.
	output, err = exec.Command("blkid", "-c", "/dev/null", "-o", "export", devicePath).CombinedOutput()
	if err != nil {
		return "", err
	}
	parseErr := errors.New("Cannot parse output of blkid.")
	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		fields := strings.Split(strings.TrimSpace(line), "=")
		if len(fields) != 2 {
			return "", parseErr
		}
		if fields[0] == "TYPE" {
			return fields[1], nil
		}
	}
	return "", parseErr
}

func isAttached(cli client.Client, pvname string) (bool, error) {
	volumeattachments := storagev1.VolumeAttachmentList{}
	err := cli.List(context.TODO(), nil, &volumeattachments)
	if err != nil {
		return true, err
	}
	for _, volumeattachment := range volumeattachments.Items {
		if *volumeattachment.Spec.Source.PersistentVolumeName != pvname {
			continue
		}
		return volumeattachment.Status.Attached, nil
	}
	return false, nil
}

func useGigaUnit(size int64) int64 {
	gs := (size + Gigabytes - 1) / Gigabytes
	return gs * Gigabytes
}

func createLvmdClient(cli client.Client, node, ds string, local bool) (*lvmdclient.Client, error) {
	addr, err := getLVMDAddr(cli, node, ds, local)
	if err != nil {
		return nil, fmt.Errorf("get lvmd addr failed, %v", err)
	}
	lvmd, err := lvmdclient.New(addr, ConnectTimeout)
	if err != nil {
		return nil, fmt.Errorf("new lvmdclient for addr %s failed, %v", addr, err)
	}
	return lvmd, nil
}

func checkVolumeExist(conn *lvmdclient.Client, vg, lv string) error {
	name := fmt.Sprintf("%s/%s", vg, lv)
	rsp, err := conn.ListLV(context.TODO(), &pb.ListLVRequest{
		VolumeGroup: name,
	})
	if err != nil {
		return err
	}
	if len(rsp.GetVolumes()) != 1 {
		return errors.New(fmt.Sprintf("failed get lvm %s", name))
	}
	return nil
}
