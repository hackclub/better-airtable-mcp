import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { McpDebugPage } from "./McpDebugPage";

function makeMemoryStorage(seed: Record<string, string> = {}) {
  const values = new Map(Object.entries(seed));
  return {
    getItem(key: string) {
      return values.has(key) ? values.get(key) ?? null : null;
    },
    setItem(key: string, value: string) {
      values.set(key, value);
    },
    removeItem(key: string) {
      values.delete(key);
    },
  };
}

function makeDebugLocation(search = "") {
  return {
    origin: "https://debug.example",
    pathname: "/debug",
    search,
    assign: vi.fn(),
    replace: vi.fn(),
  };
}

describe("McpDebugPage", () => {
  it("starts the OAuth flow by registering a debug client and redirecting to authorize", async () => {
    const fetchImpl = vi.fn().mockResolvedValueOnce({
      status: 201,
      text: async () =>
        JSON.stringify({
          client_id: "client_debug",
        }),
      headers: { get: () => null },
    } satisfies Partial<Response>);

    const storage = makeMemoryStorage();
    const location = makeDebugLocation();

    render(
      <McpDebugPage
        fetchImpl={fetchImpl as typeof fetch}
        locationImpl={location}
        storageImpl={storage}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "Connect with OAuth" }));

    await waitFor(() => {
      expect(fetchImpl).toHaveBeenCalledWith(
        "/oauth/register",
        expect.objectContaining({
          method: "POST",
          body: JSON.stringify({
            redirect_uris: ["https://debug.example/debug"],
            client_name: "Better Airtable MCP Debugger",
          }),
        }),
      );
    });

    await waitFor(() => {
      expect(location.assign).toHaveBeenCalledTimes(1);
    });

    const authorizeURL = new URL(location.assign.mock.calls[0][0] as string);
    expect(authorizeURL.pathname).toBe("/oauth/authorize");
    expect(authorizeURL.searchParams.get("response_type")).toBe("code");
    expect(authorizeURL.searchParams.get("client_id")).toBe("client_debug");
    expect(authorizeURL.searchParams.get("redirect_uri")).toBe("https://debug.example/debug");
    expect(authorizeURL.searchParams.get("code_challenge_method")).toBe("S256");
    expect(authorizeURL.searchParams.get("code_challenge")).not.toBe("");
    expect(authorizeURL.searchParams.get("state")).not.toBe("");
  });

  it("exchanges the OAuth callback code, initializes MCP, and recovers if the session disappears before a tool call", async () => {
    const storage = makeMemoryStorage();
    const startLocation = makeDebugLocation();
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce({
        status: 201,
        text: async () =>
          JSON.stringify({
            client_id: "client_debug",
          }),
        headers: { get: () => null },
      } satisfies Partial<Response>)
      .mockResolvedValueOnce({
        status: 200,
        text: async () =>
          JSON.stringify({
            access_token: "oauth-token",
          }),
        headers: { get: () => null },
      } satisfies Partial<Response>)
      .mockResolvedValueOnce({
        status: 200,
        text: async () =>
          JSON.stringify({
            jsonrpc: "2.0",
            id: 1,
            result: { protocolVersion: "2025-11-25" },
          }),
        headers: { get: (name: string) => (name === "Mcp-Session-Id" ? "session_debug" : null) },
      } satisfies Partial<Response>)
      .mockResolvedValueOnce({
        status: 202,
        text: async () => "",
        headers: { get: () => "session_debug" },
      } satisfies Partial<Response>)
      .mockResolvedValueOnce({
        status: 200,
        text: async () =>
          JSON.stringify({
            jsonrpc: "2.0",
            id: 2,
            result: {
              tools: [
                {
                  name: "list_bases",
                  description: "Search for Airtable bases the user has access to.",
                  inputSchema: { type: "object" },
                },
              ],
            },
          }),
        headers: { get: () => "session_debug" },
      } satisfies Partial<Response>)
      .mockResolvedValueOnce({
        status: 404,
        text: async () =>
          JSON.stringify({
            error: "session was not found",
          }),
        headers: { get: () => null },
      } satisfies Partial<Response>)
      .mockResolvedValueOnce({
        status: 200,
        text: async () =>
          JSON.stringify({
            jsonrpc: "2.0",
            id: 1,
            result: { protocolVersion: "2025-11-25" },
          }),
        headers: { get: (name: string) => (name === "Mcp-Session-Id" ? "session_retry" : null) },
      } satisfies Partial<Response>)
      .mockResolvedValueOnce({
        status: 202,
        text: async () => "",
        headers: { get: () => "session_retry" },
      } satisfies Partial<Response>)
      .mockResolvedValueOnce({
        status: 200,
        text: async () =>
          JSON.stringify({
            jsonrpc: "2.0",
            id: 2,
            result: {
              tools: [
                {
                  name: "list_bases",
                  description: "Search for Airtable bases the user has access to.",
                  inputSchema: { type: "object" },
                },
              ],
            },
          }),
        headers: { get: () => "session_retry" },
      } satisfies Partial<Response>)
      .mockResolvedValueOnce({
        status: 200,
        text: async () =>
          JSON.stringify({
            jsonrpc: "2.0",
            id: 100,
            result: {
              content: [
                {
                  type: "text",
                  text: "Project Tracker results\n\n1 base found.",
                },
              ],
              structuredContent: {
                bases: [{ id: "app123", name: "Project Tracker" }],
              },
            },
          }),
        headers: { get: () => "session_retry" },
      } satisfies Partial<Response>);

    const initialRender = render(
      <McpDebugPage
        fetchImpl={fetchImpl as typeof fetch}
        locationImpl={startLocation}
        storageImpl={storage}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "Connect with OAuth" }));

    await waitFor(() => {
      expect(startLocation.assign).toHaveBeenCalledTimes(1);
    });

    const authorizeURL = new URL(startLocation.assign.mock.calls[0][0] as string);
    const callbackLocation = makeDebugLocation(
      `?code=oauth-code&state=${authorizeURL.searchParams.get("state")}`,
    );

    initialRender.unmount();

    render(
      <McpDebugPage
        fetchImpl={fetchImpl as typeof fetch}
        locationImpl={callbackLocation}
        storageImpl={storage}
      />,
    );

    await waitFor(() => {
      expect(screen.getByLabelText("Bearer Token")).toHaveValue("oauth-token");
    });
    expect(callbackLocation.replace).toHaveBeenCalledWith("/debug");

    fireEvent.click(screen.getByRole("button", { name: "Initialize + Load Tools" }));

    expect(await screen.findByText("Search for Airtable bases the user has access to.")).toBeInTheDocument();
    expect(screen.getByText("session_debug")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Execute list_bases" }));

    await waitFor(() => {
      expect(screen.getByText(/"Project Tracker"/)).toBeInTheDocument();
    });
    expect(screen.getByText("session_retry")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Content Text" }));

    expect(
      screen.getByText((content) => content.includes("Project Tracker results")),
    ).toBeInTheDocument();
    expect(
      screen.getByText((content) => content.includes("1 base found.")),
    ).toBeInTheDocument();

    expect(fetchImpl).toHaveBeenNthCalledWith(
      2,
      "/oauth/token",
      expect.objectContaining({
        method: "POST",
        body: expect.stringContaining("grant_type=authorization_code"),
      }),
    );
    expect(fetchImpl).toHaveBeenNthCalledWith(
      3,
      "/mcp",
      expect.objectContaining({
        method: "POST",
        headers: expect.objectContaining({
          Authorization: "Bearer oauth-token",
        }),
      }),
    );
    expect(fetchImpl).toHaveBeenNthCalledWith(
      5,
      "/mcp",
      expect.objectContaining({
        headers: expect.objectContaining({
          Authorization: "Bearer oauth-token",
          "Mcp-Session-Id": "session_debug",
        }),
      }),
    );
    expect(fetchImpl).toHaveBeenNthCalledWith(
      6,
      "/mcp",
      expect.objectContaining({
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: 100,
          method: "tools/call",
          params: {
            name: "list_bases",
            arguments: { query: "project" },
          },
        }),
        headers: expect.objectContaining({
          Authorization: "Bearer oauth-token",
          "Mcp-Session-Id": "session_debug",
        }),
      }),
    );
    expect(fetchImpl).toHaveBeenNthCalledWith(
      7,
      "/mcp",
      expect.objectContaining({
        headers: expect.objectContaining({
          Authorization: "Bearer oauth-token",
        }),
      }),
    );
    expect(fetchImpl).toHaveBeenNthCalledWith(
      9,
      "/mcp",
      expect.objectContaining({
        headers: expect.objectContaining({
          Authorization: "Bearer oauth-token",
          "Mcp-Session-Id": "session_retry",
        }),
      }),
    );
    expect(fetchImpl).toHaveBeenNthCalledWith(
      10,
      "/mcp",
      expect.objectContaining({
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: 100,
          method: "tools/call",
          params: {
            name: "list_bases",
            arguments: { query: "project" },
          },
        }),
      }),
    );
  });
});
