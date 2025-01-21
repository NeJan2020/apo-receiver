package ebpffile

import (
	"context"
	"testing"

	"github.com/CloudDetail/apo-receiver/pkg/model"
	"github.com/stretchr/testify/assert"
)

func TestFetchEbpfFile(t *testing.T) {
	fileService := NewEbpfFIleServer("localhost:8080", "")
	kernelVersion := "5.15.158-1.el7.x86_64"
	resp, err := fileService.GetFile(context.Background(), &model.FileRequest{
		AgentVersion:  "v0.1.0",
		KernelVersion: kernelVersion,
		OsVersion:     "Linux-7-CentOS-Linux",
	})
	assert.Nil(t, err, "fetch ebpf file error")
	assert.NotEqual(t, len(resp.FileContent), 0, "file content is empty")
	assert.Equal(t, resp.FileName, kernelVersion+".o")
}

func TestFetchEbpfFileV1(t *testing.T) {
	fileService := NewEbpfFIleServer("localhost:8080", "")
	kernelVersion := "5.4.8-1.el7.elrepo.x86_64"
	resp, err := fileService.GetFile(context.Background(), &model.FileRequest{
		AgentVersion:   "v0.1.0",
		KernelVersion:  kernelVersion,
		OsDistribution: "centos",
		Arch:           "x86_64",
	})
	assert.Nil(t, err, "fetch ebpf file error")
	assert.NotEqual(t, len(resp.FileContent), 0, "file content is empty")
	assert.Equal(t, resp.FileName, kernelVersion+".o")
}

func TestFetchEbpfFileV1Compatible(t *testing.T) {
	fileService := NewEbpfFIleServer("localhost:8080", "")
	kernelVersion := "5.15.158-1.el7.x86_64"
	resp, err := fileService.GetFile(context.Background(), &model.FileRequest{
		AgentVersion:   "v0.1.0",
		KernelVersion:  kernelVersion,
		OsVersion:      "Linux-7-CentOS-Linux",
		OsDistribution: "centos",
		Arch:           "x86_64",
	})
	assert.Nil(t, err, "fetch ebpf file error")
	assert.NotEqual(t, len(resp.FileContent), 0, "file content is empty")
	assert.Equal(t, resp.FileName, kernelVersion+".o")
}
