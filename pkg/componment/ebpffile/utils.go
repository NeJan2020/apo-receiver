package ebpffile

import (
	"os"
	"path"

	"github.com/CloudDetail/apo-receiver/pkg/model"
)

type EbpfFetchAPI int

const (
	EBPF_FETCH_API EbpfFetchAPI = iota
	EBPF_FETCH_API_V1
)

func checkVersion(osDistribution, arch string) EbpfFetchAPI {
	if osDistribution != "" && arch != "" {
		return EBPF_FETCH_API_V1
	}
	return EBPF_FETCH_API
}

// found ebpf file in local
func checkLocalEbpfFile(req *model.FileRequest, version EbpfFetchAPI) (string, error) {
	var filePath string
	if version == EBPF_FETCH_API_V1 {
		filePath = path.Join("/opt", req.AgentVersion, req.OsDistribution, req.Arch, req.KernelVersion+".o")
		_, err := os.Stat(filePath)
		if err != nil {
			return filePath, err
		}
	} else {
		filePath = path.Join("/opt", req.AgentVersion, req.OsVersion, req.KernelVersion+".o")
		_, err := os.Stat(filePath)
		if err != nil {
			return filePath, err
		}
	}
	return filePath, nil
}
