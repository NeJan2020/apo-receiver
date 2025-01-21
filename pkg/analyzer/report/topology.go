package report

import (
	apmclient "github.com/CloudDetail/apo-module/apm/client/v1"
	apmmodel "github.com/CloudDetail/apo-module/apm/model/v1"
	"github.com/CloudDetail/apo-module/model/v1"
	"github.com/CloudDetail/apo-receiver/pkg/analyzer/external"
)

type Topology struct {
	Nodes []*TopologyNode
}

func NewTopology(apmType string, services []*apmmodel.OtelServiceNode, sampledTraces map[string]*model.Trace, factory *external.ExternalFactory) *Topology {
	nodes := make([]*TopologyNode, 0)
	for _, service := range services {
		node := collectChildTopologyNode(apmType, nil, nil, service, sampledTraces, factory)
		nodes = append(nodes, node)
	}
	return &Topology{
		Nodes: nodes,
	}
}

func collectChildTopologyNode(apmType string, parent *TopologyNode, parentService *apmmodel.OtelServiceNode, service *apmmodel.OtelServiceNode, sampledTraces map[string]*model.Trace, factory *external.ExternalFactory) *TopologyNode {
	serviceNode := newServerTopologyNode(apmType, parent, parentService, service, sampledTraces, factory)
	for _, child := range service.Children {
		collectChildTopologyNode(apmType, serviceNode, service, child, sampledTraces, factory)
	}
	return serviceNode
}

type TopologyNode struct {
	StartTime   uint64
	ServiceName string
	Url         string
	SpanId      string
	SideSpanId  string
	TopNode     bool
	NodeName    string
	NodeIp      string
	Pid         int
	ContainerId string
	IsTraced    bool
	Children    []*TopologyNode
	Externals   []*external.External
	Parent      *TopologyNode
}

func newServerTopologyNode(apmType string, parent *TopologyNode, parentService *apmmodel.OtelServiceNode, service *apmmodel.OtelServiceNode, sampledTraces map[string]*model.Trace, factory *external.ExternalFactory) *TopologyNode {
	var (
		startTime   uint64
		serviceName string
		url         string
		spanId      string
		nodeName    string
		nodeIp      string
		pid         int
		containerId string
		isTraced    bool
	)

	if sampledSpan := apmclient.GetMatchSampledSpanTrace(apmType, service, sampledTraces); sampledSpan != nil {
		startTime = sampledSpan.Labels.StartTime
		serviceName = sampledSpan.Labels.ServiceName
		url = sampledSpan.Labels.Url
		spanId = sampledSpan.Labels.ApmSpanId
		nodeName = sampledSpan.Labels.NodeName
		nodeIp = sampledSpan.Labels.NodeIp
		pid = int(sampledSpan.Labels.Pid)
		containerId = sampledSpan.Labels.ContainerId
		isTraced = true
	} else {
		entrySpan := service.GetEntrySpan()
		startTime = entrySpan.StartTime
		serviceName = entrySpan.ServiceName
		url = entrySpan.Name
		spanId = entrySpan.SpanId
		nodeName = "unknown"
		nodeIp = "unknown"
		containerId = ""
		pid = 0
		isTraced = false
	}

	node := &TopologyNode{
		StartTime:   startTime,
		ServiceName: serviceName,
		Url:         url,
		SpanId:      spanId,
		SideSpanId:  getSideSpanId(parentService, service),
		TopNode:     service.IsTopNode(),
		NodeName:    nodeName,
		NodeIp:      nodeIp,
		Pid:         pid,
		ContainerId: containerId,
		IsTraced:    isTraced,
		Children:    make([]*TopologyNode, 0),
		Externals:   factory.BuildExternals(service),
		Parent:      parent,
	}
	if parent != nil {
		parent.AddChild(node)
	}
	return node
}

func (node *TopologyNode) AddChild(child *TopologyNode) {
	node.Children = append(node.Children, child)
	child.Parent = node
}

func (node *TopologyNode) GetParentSideExternal() *external.External {
	if node.Parent == nil || node.SideSpanId == "" {
		return nil
	}
	for _, externalData := range node.Parent.Externals {
		if externalData.SpanId == node.SideSpanId {
			return externalData
		}
	}
	return nil
}

func getSideSpanId(parentService *apmmodel.OtelServiceNode, serviceNode *apmmodel.OtelServiceNode) string {
	if parentService == nil {
		return ""
	}
	for _, exitSpan := range parentService.ExitSpans {
		if serviceNode.MatchEntrySpan(exitSpan.NextSpanId) {
			return exitSpan.SpanId
		}
	}
	return ""
}
