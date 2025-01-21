package report

import (
	"fmt"

	"github.com/CloudDetail/apo-module/model/v1"
)

type ErrorPropagation struct {
	ErrorNodes []*ErrorNode
}

func NewErrorPropagation(node *model.ErrorTreeNode) *ErrorPropagation {
	propagation := &ErrorPropagation{
		ErrorNodes: make([]*ErrorNode, 0),
	}

	propagation.collectErrorNodes(node, "", 0, 1)
	return propagation
}

func (propagation *ErrorPropagation) collectErrorNodes(node *model.ErrorTreeNode, path string, index int, depth int) {
	currentPath := fmt.Sprintf("%s%d.", path, index)

	types := make([]string, 0)
	messages := make([]string, 0)
	for _, errorSpan := range node.ErrorSpans {
		for _, exception := range errorSpan.Exceptions {
			types = append(types, exception.Type)
			messages = append(messages, exception.Message)
		}
	}

	propagation.ErrorNodes = append(propagation.ErrorNodes, &ErrorNode{
		Service:       node.ServiceName,
		Instance:      node.Id,
		Url:           node.Url,
		IsError:       node.IsError,
		IsTraced:      node.IsTraced,
		ErrorTypes:    types,
		ErrorMessages: messages,
		Depth:         depth,
		Path:          currentPath,
	})
	for i, child := range node.Children {
		propagation.collectErrorNodes(child, currentPath, i, depth+1)
	}
}

func (propagation *ErrorPropagation) GetServiceList() []string {
	result := make([]string, 0)
	for _, node := range propagation.ErrorNodes {
		result = append(result, node.Service)
	}
	return result
}

func (propagation *ErrorPropagation) GetInstanceList() []string {
	result := make([]string, 0)
	for _, node := range propagation.ErrorNodes {
		result = append(result, node.Instance)
	}
	return result
}

func (propagation *ErrorPropagation) GetUrlList() []string {
	result := make([]string, 0)
	for _, node := range propagation.ErrorNodes {
		result = append(result, node.Url)
	}
	return result
}

func (propagation *ErrorPropagation) GetErrorTypeList() [][]string {
	result := make([][]string, 0)
	for _, node := range propagation.ErrorNodes {
		result = append(result, node.ErrorTypes)
	}
	return result
}

func (propagation *ErrorPropagation) GetErrorMessageList() [][]string {
	result := make([][]string, 0)
	for _, node := range propagation.ErrorNodes {
		result = append(result, node.ErrorMessages)
	}
	return result
}

func (propagation *ErrorPropagation) GetIsErrorList() []bool {
	result := make([]bool, 0)
	for _, node := range propagation.ErrorNodes {
		result = append(result, node.IsError)
	}
	return result
}

func (propagation *ErrorPropagation) GetIsTracedList() []bool {
	result := make([]bool, 0)
	for _, node := range propagation.ErrorNodes {
		result = append(result, node.IsTraced)
	}
	return result
}

func (propagation *ErrorPropagation) GetDepthList() []int {
	result := make([]int, 0)
	for _, node := range propagation.ErrorNodes {
		result = append(result, node.Depth)
	}
	return result
}

func (propagation *ErrorPropagation) GetPathList() []string {
	result := make([]string, 0)
	for _, node := range propagation.ErrorNodes {
		result = append(result, node.Path)
	}
	return result
}

type ErrorNode struct {
	Service       string
	Instance      string
	Url           string
	IsError       bool
	IsTraced      bool
	ErrorTypes    []string
	ErrorMessages []string
	Depth         int
	Path          string
}
