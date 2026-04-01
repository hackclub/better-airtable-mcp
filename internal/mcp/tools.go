package mcp

import (
	"context"
	"encoding/json"
)

type Tool interface {
	Definition() ToolDefinition
	Call(context.Context, json.RawMessage) (ToolCallResult, error)
}

type ToolDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

type ToolCallResult struct {
	Content           []ToolContent `json:"content,omitempty"`
	StructuredContent any           `json:"structuredContent,omitempty"`
	IsError           bool          `json:"isError,omitempty"`
}

type ToolContent struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

func TextResult(text string, structured any) ToolCallResult {
	return ToolCallResult{
		Content:           []ToolContent{{Type: "text", Text: text}},
		StructuredContent: structured,
	}
}

func ErrorResult(text string, structured any) ToolCallResult {
	result := TextResult(text, structured)
	result.IsError = true
	return result
}
