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
    expect(screen.getAllByText("Website Redesign")).toHaveLength(2);
    expect(screen.getByText("Done")).toBeInTheDocument();
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
      expect(
        screen.getByText((content) =>
          content.replace(/\s+/g, " ").includes("Status: completed"),
        ),
      ).toBeInTheDocument();
    });

    expect(fetchImpl).toHaveBeenNthCalledWith(
      2,
      "/api/operations/op_123/approve",
      expect.objectContaining({
        method: "POST",
      }),
    );
  });
});
