import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import App from "./App";
import type { OperationView } from "./types";

function makeOperation(overrides: Partial<OperationView> = {}): OperationView {
  return {
    operation_id: "op_123",
    status: "pending_approval",
    approval_url: "https://example.test/approve/op_123",
    base_id: "app123",
    base_name: "Project Tracker",
    mcp_session_id: "session_123",
    mcp_client_id: "client_123",
    mcp_client_name: "Claude",
    summary: "Update 1 record in projects",
    created_at: "2026-04-01T12:00:00Z",
    expires_at: "2099-04-01T12:10:00Z",
    last_synced_at: "2026-04-01T11:59:00Z",
    operations: [
      {
        type: "update_records",
        table: "projects",
        original_table_name: "Projects",
        records: [
          {
            id: "rec1",
            current_fields: {
              name: "Website Redesign",
              status: "Planning",
            },
            fields: {
              name: "Website Redesign",
              status: "Done",
            },
          },
        ],
      },
    ],
    approval_url_is_credential: true,
    preview_is_snapshot: true,
    can_approve: true,
    can_reject: true,
    ...overrides,
  };
}

describe("App", () => {
  it("renders approval operation details from the API", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => makeOperation(),
    } satisfies Partial<Response>);

    render(<App pathname="/approve/op_123" fetchImpl={fetchImpl as typeof fetch} />);

    expect(await screen.findByText("Update 1 record in projects")).toBeInTheDocument();
    expect(screen.getByText("Claude (client_123)")).toBeInTheDocument();
    // Only the changed field appears: status Planning -> Done. Unchanged "name" is folded away.
    expect(screen.getByText("Planning")).toBeInTheDocument();
    expect(screen.getByText("Done")).toBeInTheDocument();
    expect(screen.queryByText("Website Redesign")).toBeNull();
  });

  it("posts approval actions and refreshes the rendered status", async () => {
    const approved = makeOperation({
      status: "completed",
      can_approve: false,
      can_reject: false,
      result: {
        completed_batches: 1,
        updated_record_ids: ["rec1"],
      },
    });

    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        json: async () => makeOperation(),
      } satisfies Partial<Response>)
      .mockResolvedValueOnce({
        ok: true,
        json: async () => approved,
      } satisfies Partial<Response>);

    render(<App pathname="/approve/op_123" fetchImpl={fetchImpl as typeof fetch} />);

    fireEvent.click(await screen.findByRole("button", { name: "Approve" }));

    await waitFor(() => {
      expect(screen.getByText("Changes applied")).toBeInTheDocument();
    });

    expect(fetchImpl).toHaveBeenNthCalledWith(
      2,
      "/api/operations/op_123/approve",
      expect.objectContaining({
        method: "POST",
      }),
    );
  });

  it("posts rejections and renders the rejected status", async () => {
    const rejected = makeOperation({
      status: "rejected",
      can_approve: false,
      can_reject: false,
      resolved_at: "2026-04-01T12:05:00Z",
    });

    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        json: async () => makeOperation(),
      } satisfies Partial<Response>)
      .mockResolvedValueOnce({
        ok: true,
        json: async () => rejected,
      } satisfies Partial<Response>);

    render(<App pathname="/approve/op_123" fetchImpl={fetchImpl as typeof fetch} />);

    fireEvent.click(await screen.findByRole("button", { name: "Reject" }));

    await waitFor(() => {
      expect(screen.getByText("Rejected")).toBeInTheDocument();
    });

    expect(fetchImpl).toHaveBeenNthCalledWith(
      2,
      "/api/operations/op_123/reject",
      expect.objectContaining({
        method: "POST",
      }),
    );

    // Once rejected, no further action is offered.
    expect(screen.queryByRole("button", { name: "Approve" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Reject" })).toBeNull();
  });

  it("renders an expired operation clearly, with the preview but no actions", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({
      ok: true,
      json: async () =>
        makeOperation({
          status: "expired",
          can_approve: false,
          can_reject: false,
          expires_at: "2026-04-01T12:10:00Z",
        }),
    } satisfies Partial<Response>);

    render(<App pathname="/approve/op_123" fetchImpl={fetchImpl as typeof fetch} />);

    expect(await screen.findByText("Request expired")).toBeInTheDocument();
    // The reviewer can still see what was requested...
    expect(screen.getByText("Planning")).toBeInTheDocument();
    expect(screen.getByText("Done")).toBeInTheDocument();
    // ...but can no longer act on it.
    expect(screen.queryByRole("button", { name: "Approve" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Reject" })).toBeNull();
  });

  it("renders the API error message when loading the operation fails", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({
      ok: false,
      json: async () => ({ error: "operation not found" }),
    } satisfies Partial<Response>);

    render(<App pathname="/approve/op_missing" fetchImpl={fetchImpl as typeof fetch} />);

    expect(await screen.findByText("operation not found")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Approve" })).toBeNull();
  });

  it("renders a network failure as an error state instead of loading forever", async () => {
    const fetchImpl = vi.fn().mockRejectedValue(new Error("network down"));

    render(<App pathname="/approve/op_123" fetchImpl={fetchImpl as typeof fetch} />);

    expect(await screen.findByText("Error: network down")).toBeInTheDocument();
  });

  it("explains a link with no operation ID without calling the API", () => {
    const fetchImpl = vi.fn();

    render(<App pathname="/approve" fetchImpl={fetchImpl as unknown as typeof fetch} />);

    expect(
      screen.getByText("This approval link is missing an operation ID."),
    ).toBeInTheDocument();
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it("renders the MCP debugger shell on /debug", () => {
    render(<App pathname="/debug" fetchImpl={vi.fn() as typeof fetch} />);

    expect(screen.getByText("MCP Tool Debugger")).toBeInTheDocument();
    expect(screen.getByLabelText("Bearer Token")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Connect with OAuth" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Initialize + load tools" })).toBeDisabled();
  });
});
