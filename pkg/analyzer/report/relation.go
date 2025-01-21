package report

import (
	"fmt"
	"log"

	"github.com/CloudDetail/apo-module/apm/model/v1"
	"github.com/CloudDetail/apo-receiver/pkg/analyzer/external"
)

type Relation struct {
	TraceId       string
	RootNode      *TopologyNode
	Relationships []*Relationship
}

func NewRelation(traceId string, node *TopologyNode) *Relation {
	return &Relation{
		TraceId:       traceId,
		RootNode:      node,
		Relationships: make([]*Relationship, 0),
	}
}

func (relation *Relation) CollectRelationships() {
	if len(relation.Relationships) == 0 {
		relation.collectRelationship(relation.RootNode, fmt.Sprintf("%s_", relation.RootNode.SpanId), 0)
	}
}

func (relation *Relation) CollectExternalNodes() []*TopologyNode {
	nodes := make([]*TopologyNode, 0)
	collectExternalNodes(relation.RootNode, &nodes)
	return nodes
}

func collectExternalNodes(node *TopologyNode, nodes *[]*TopologyNode) {
	if len(node.Externals) > 0 {
		*nodes = append(*nodes, node)
	}
	for _, child := range node.Children {
		collectExternalNodes(child, nodes)
	}
}

func (relation *Relation) collectRelationship(topologyNode *TopologyNode, path string, index int) {
	currentPath := fmt.Sprintf("%s%d.", path, index)
	var serviceSide *external.External
	clientSides := make([]*external.External, 0)

	for _, externalData := range topologyNode.Externals {
		if externalData.Kind == model.SpanKindConsumer {
			serviceSide = externalData
		} else if externalData.NextSpanId == "" {
			clientSides = append(clientSides, externalData)
		}
	}

	if serviceSide == nil {
		serviceSide = topologyNode.GetParentSideExternal()
	}

	if topologyNode.Parent != nil && serviceSide == nil {
		log.Printf("[x Miss RelationSide] %s -> %s, traceId: %s, spanId: %s",
			topologyNode.Parent.ServiceName,
			topologyNode.ServiceName,
			relation.TraceId,
			topologyNode.SpanId)
		return
	}

	relation.Relationships = append(relation.Relationships, NewServerRelationship(currentPath, serviceSide, topologyNode))
	childNodeCount := len(topologyNode.Children)
	for i, clientSide := range clientSides {
		relation.Relationships = append(relation.Relationships, NewClientRelationship(fmt.Sprintf("%s%d.", currentPath, childNodeCount+i), clientSide, topologyNode))
	}

	for i, child := range topologyNode.Children {
		relation.collectRelationship(child, currentPath, i)
	}
}

type Relationship struct {
	Path          string
	ParentService string
	ParentUrl     string
	ParentTraced  bool
	ClientGroup   string
	ClientType    string
	ClientPeer    string
	ClientKey     string
	Service       string
	Url           string
	IsTraced      bool
	IsAsync       bool
}

func NewClientRelationship(path string, client *external.External, node *TopologyNode) *Relationship {
	return &Relationship{
		Path:          path,
		ParentService: node.ServiceName,
		ParentUrl:     node.Url,
		ClientGroup:   client.Group,
		ClientType:    client.Type,
		ClientPeer:    client.Peer,
		ClientKey:     client.Name,
		Service:       "",
		Url:           "",
		IsTraced:      false,
		IsAsync:       false,
	}
}

func NewServerRelationship(path string, client *external.External, node *TopologyNode) *Relationship {
	var (
		parentService string
		parentUrl     string
		parentTraced  bool
		clientGroup   string
		clientType    string
		clientPeer    string
		clientKey     string
		isAsync       bool
	)

	if node.Parent != nil {
		parentService = node.Parent.ServiceName
		parentUrl = node.Parent.Url
		parentTraced = node.Parent.IsTraced
	}
	if client != nil {
		clientGroup = client.Group
		clientType = client.Type
		clientPeer = client.Peer
		clientKey = client.Name
		isAsync = client.Group == "mq"
	}
	return &Relationship{
		Path:          path,
		ParentService: parentService,
		ParentUrl:     parentUrl,
		ParentTraced:  parentTraced,
		ClientGroup:   clientGroup,
		ClientType:    clientType,
		ClientPeer:    clientPeer,
		ClientKey:     clientKey,
		Service:       node.ServiceName,
		Url:           node.Url,
		IsTraced:      node.IsTraced,
		IsAsync:       isAsync,
	}
}

// For test
func newRelationship(path string) *Relationship {
	return &Relationship{
		Path: path,
	}
}

func (relation *Relationship) withService(service string, url string) *Relationship {
	relation.Service = service
	relation.Url = url
	return relation
}

func (relation *Relationship) withParent(parentService string, parentUrl string) *Relationship {
	relation.ParentService = parentService
	relation.ParentUrl = parentUrl
	return relation
}

func (relation *Relationship) withExternalClient(clientType string, clientPeer string, clientKey string) *Relationship {
	relation.ClientGroup = "external"
	relation.ClientType = clientType
	relation.ClientPeer = clientPeer
	relation.ClientKey = clientKey
	return relation
}

func (relation *Relationship) withDbClient(clientType string, clientPeer string, clientKey string) *Relationship {
	relation.ClientGroup = "db"
	relation.ClientType = clientType
	relation.ClientPeer = clientPeer
	relation.ClientKey = clientKey
	return relation
}

func (relation *Relationship) withMqClient(clientType string, clientPeer string, clientKey string) *Relationship {
	relation.ClientGroup = "mq"
	relation.ClientType = clientType
	relation.ClientPeer = clientPeer
	relation.ClientKey = clientKey
	return relation
}

func (relation *Relationship) withAsync() *Relationship {
	relation.IsAsync = true
	return relation
}

type ServiceNode struct {
	Path          string
	ParentService string
	ParentUrl     string
	Service       string
	Url           string
	IsTraced      bool
}

func NewServiceNode(path string, parentService string, parentUrl string, service string, url string, isTraced bool) *ServiceNode {
	return &ServiceNode{
		Path:          path,
		ParentService: parentService,
		ParentUrl:     parentUrl,
		Service:       service,
		Url:           url,
		IsTraced:      isTraced,
	}
}
