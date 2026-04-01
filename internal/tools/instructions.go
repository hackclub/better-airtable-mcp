package tools

const approvalURLAssistantInstruction = "The user must click the approval_url to approve this operation. Always present it as a clickable link in your response — the user cannot see tool results directly."

func AssistantInstructionForApprovalURL() string {
	return approvalURLAssistantInstruction
}
