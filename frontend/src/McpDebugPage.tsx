import { useEffect, useMemo, useState } from "react";

interface McpDebugPageProps {
  fetchImpl?: typeof fetch;
  locationImpl?: DebugLocation;
  storageImpl?: DebugStorage | null;
}

interface DebugStorage {
  getItem: (key: string) => string | null;
  setItem: (key: string, value: string) => void;
  removeItem: (key: string) => void;
}

interface DebugLocation {
  origin: string;
  pathname: string;
  search: string;
  assign: (url: string) => void;
  replace: (url: string) => void;
}

interface McpToolDefinition {
  name: string;
  description: string;
  inputSchema?: Record<string, unknown>;
}

interface McpResponseSnapshot {
  requestBody: string;
  status: number;
  responseText: string;
}

type ResponseViewMode = "full" | "content_text";

const tokenStorageKey = "better-airtable-mcp.debug.bearer_token";
const oauthClientIDStorageKey = "better-airtable-mcp.debug.oauth_client_id";
const oauthStateStorageKey = "better-airtable-mcp.debug.oauth_state";
const oauthVerifierStorageKey = "better-airtable-mcp.debug.oauth_verifier";
const sessionHeader = "Mcp-Session-Id";
const oauthClientName = "Better Airtable MCP Debugger";

function prettyJSON(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

function parseJSON(text: string): unknown | null {
  if (text.trim() === "") {
    return null;
  }

  try {
    return JSON.parse(text) as unknown;
  } catch {
    return null;
  }
}

function getDefaultStorage(): DebugStorage | null {
  if (typeof window === "undefined") {
    return null;
  }

  const candidate = (window as Window & { localStorage?: unknown }).localStorage;
  if (
    !candidate ||
    typeof candidate !== "object" ||
    typeof (candidate as DebugStorage).getItem !== "function" ||
    typeof (candidate as DebugStorage).setItem !== "function" ||
    typeof (candidate as DebugStorage).removeItem !== "function"
  ) {
    return null;
  }

  return candidate as DebugStorage;
}

function getDefaultLocation(): DebugLocation {
  return {
    origin: window.location.origin,
    pathname: window.location.pathname,
    search: window.location.search,
    assign: (url: string) => window.location.assign(url),
    replace: (url: string) => window.history.replaceState(null, "", url),
  };
}

function readStoredValue(storage: DebugStorage | null, key: string): string {
  if (!storage) {
    return "";
  }
  return storage.getItem(key) ?? "";
}

function writeStoredValue(storage: DebugStorage | null, key: string, value: string): void {
  if (!storage) {
    return;
  }
  storage.setItem(key, value);
}

function clearStoredValue(storage: DebugStorage | null, key: string): void {
  if (!storage) {
    return;
  }
  storage.removeItem(key);
}

function clearOAuthFlowState(storage: DebugStorage | null): void {
  clearStoredValue(storage, oauthStateStorageKey);
  clearStoredValue(storage, oauthVerifierStorageKey);
}

function debugRedirectURI(location: DebugLocation): string {
  return new URL("/debug", location.origin).toString();
}

function base64URL(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function requireCrypto(): Crypto {
  if (
    typeof globalThis.crypto === "undefined" ||
    typeof globalThis.crypto.getRandomValues !== "function" ||
    typeof globalThis.crypto.subtle === "undefined"
  ) {
    throw new Error("Browser crypto is required for the debug OAuth flow.");
  }
  return globalThis.crypto;
}

function randomHex(byteLength: number): string {
  const cryptoImpl = requireCrypto();
  const bytes = new Uint8Array(byteLength);
  cryptoImpl.getRandomValues(bytes);
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function s256Challenge(verifier: string): Promise<string> {
  const cryptoImpl = requireCrypto();
  const digest = await cryptoImpl.subtle.digest("SHA-256", new TextEncoder().encode(verifier));
  return base64URL(new Uint8Array(digest));
}

function defaultArgumentsForTool(toolName: string): string {
  switch (toolName) {
    case "list_bases":
      return prettyJSON({ query: "project" });
    case "list_schema":
      return prettyJSON({ base: "appXXXXXXXXXXXXXX" });
    case "query":
      return prettyJSON({
        base: "appXXXXXXXXXXXXXX",
        sql: ["SELECT * FROM projects LIMIT 5"],
      });
    case "mutate":
      return prettyJSON({
        base: "appXXXXXXXXXXXXXX",
        operations: [
          {
            type: "create_records",
            table: "projects",
            records: [
              {
                fields: {
                  name: "Debug-created record",
                },
              },
            ],
          },
        ],
      });
    case "sync":
      return prettyJSON({ base: "appXXXXXXXXXXXXXX" });
    case "check_operation":
      return prettyJSON({ operation_id: "sync_appXXXXXXXXXXXXXX" });
    default:
      return prettyJSON({});
  }
}

async function readResponse(response: Response) {
  const responseText = await response.text();
  const parsed = parseJSON(responseText);
  return {
    status: response.status,
    responseText: parsed === null ? responseText : prettyJSON(parsed),
    parsed,
    sessionID: response.headers.get(sessionHeader) ?? "",
  };
}

function isMissingSession(response: { status: number; responseText: string }): boolean {
  return response.status === 404 && response.responseText.includes("session was not found");
}

function extractContentText(responseText: string): string {
  const parsed = parseJSON(responseText) as
    | {
        result?: {
          content?: Array<{ type?: string; text?: string }>;
        };
      }
    | null;
  const content = parsed?.result?.content;
  if (!Array.isArray(content)) {
    return "";
  }

  return content
    .filter((item) => item?.type === "text" && typeof item.text === "string")
    .map((item) => item.text?.trim() ?? "")
    .filter((item) => item !== "")
    .join("\n\n");
}

function ToolCard({
  tool,
  argumentsText,
  onArgumentsChange,
  onExecute,
  busy,
  response,
}: {
  tool: McpToolDefinition;
  argumentsText: string;
  onArgumentsChange: (value: string) => void;
  onExecute: () => void;
  busy: boolean;
  response: McpResponseSnapshot | null;
}) {
  const textareaID = useMemo(() => `tool-arguments-${tool.name}`, [tool.name]);
  const [responseViewMode, setResponseViewMode] = useState<ResponseViewMode>("full");
  const contentText = useMemo(
    () => (response ? extractContentText(response.responseText) : ""),
    [response],
  );

  return (
    <section className="preview-card tool-card">
      <div className="tool-card-header">
        <div>
          <div className="pill-row debug-pill-row">
            <span className="pill">{tool.name}</span>
          </div>
          <h2>{tool.name}</h2>
          <p className="preview-subtitle">{tool.description}</p>
        </div>
        <button
          aria-label={`Execute ${tool.name}`}
          className="action-button approve"
          disabled={busy}
          onClick={onExecute}
        >
          {busy ? "Running..." : "Execute"}
        </button>
      </div>

      <div className="debug-grid">
        <div className="debug-panel">
          <label className="debug-label" htmlFor={textareaID}>
            Arguments JSON
          </label>
          <textarea
            id={textareaID}
            className="debug-textarea"
            value={argumentsText}
            onChange={(event) => onArgumentsChange(event.target.value)}
            spellCheck={false}
          />
        </div>

        <div className="debug-panel">
          <div className="debug-label">Input Schema</div>
          <pre className="debug-pre">
            {prettyJSON(tool.inputSchema ?? { type: "object", properties: {} })}
          </pre>
        </div>
      </div>

      <div className="debug-panel tool-response-panel">
        <div className="debug-label">Last Response</div>
        {response ? (
          <>
            <div className="pill-row debug-pill-row">
              <span className="pill">HTTP {response.status}</span>
            </div>
            <div className="debug-response-grid">
              <div>
                <div className="debug-subtitle">Request</div>
                <pre className="debug-pre">{response.requestBody}</pre>
              </div>
              <div>
                <div className="debug-toggle-row">
                  <div className="debug-subtitle">Response</div>
                  <div className="debug-toggle-group" role="tablist" aria-label={`${tool.name} response view`}>
                    <button
                      className={`debug-toggle-button ${responseViewMode === "full" ? "active" : ""}`}
                      type="button"
                      onClick={() => setResponseViewMode("full")}
                    >
                      Full JSON
                    </button>
                    <button
                      className={`debug-toggle-button ${responseViewMode === "content_text" ? "active" : ""}`}
                      type="button"
                      disabled={contentText === ""}
                      onClick={() => setResponseViewMode("content_text")}
                    >
                      Content Text
                    </button>
                  </div>
                </div>
                <pre className="debug-pre">
                  {responseViewMode === "content_text"
                    ? contentText || "No result.content[].text value was returned."
                    : response.responseText}
                </pre>
              </div>
            </div>
          </>
        ) : (
          <p className="meta-text">No response yet.</p>
        )}
      </div>
    </section>
  );
}

export function McpDebugPage({
  fetchImpl = window.fetch.bind(window),
  locationImpl,
  storageImpl,
}: McpDebugPageProps) {
  const effectiveLocation = useMemo(() => locationImpl ?? getDefaultLocation(), [locationImpl]);
  const effectiveStorage = useMemo(
    () => (storageImpl !== undefined ? storageImpl : getDefaultStorage()),
    [storageImpl],
  );

  const [bearerToken, setBearerToken] = useState(() => readStoredValue(effectiveStorage, tokenStorageKey));
  const [oauthClientID, setOAuthClientID] = useState(() =>
    readStoredValue(effectiveStorage, oauthClientIDStorageKey),
  );
  const [sessionID, setSessionID] = useState("");
  const [tools, setTools] = useState<McpToolDefinition[]>([]);
  const [toolArguments, setToolArguments] = useState<Record<string, string>>({});
  const [toolResponses, setToolResponses] = useState<Record<string, McpResponseSnapshot | null>>({});
  const [busyToolName, setBusyToolName] = useState("");
  const [busyConnect, setBusyConnect] = useState(false);
  const [busyOAuth, setBusyOAuth] = useState(false);
  const [connectionError, setConnectionError] = useState("");
  const [oauthStatus, setOAuthStatus] = useState("");
  const [oauthResponseText, setOAuthResponseText] = useState("");
  const [toolsResponseText, setToolsResponseText] = useState("");

  useEffect(() => {
    writeStoredValue(effectiveStorage, tokenStorageKey, bearerToken);
  }, [bearerToken, effectiveStorage]);

  useEffect(() => {
    setSessionID("");
    setTools([]);
    setToolResponses({});
    setToolsResponseText("");
  }, [bearerToken]);

  async function sendRPC(
    body: Record<string, unknown>,
    token: string,
    currentSessionID = "",
  ) {
    const response = await fetchImpl("/mcp", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
        ...(currentSessionID ? { [sessionHeader]: currentSessionID } : {}),
      },
      body: JSON.stringify(body),
    });
    return readResponse(response);
  }

  async function connectWithToken(token: string): Promise<string> {
    const trimmedToken = token.trim();
    if (trimmedToken === "") {
      setConnectionError("Connect with OAuth first so the page has a bearer token.");
      throw new Error("missing bearer token");
    }

    setBusyConnect(true);
    setConnectionError("");
    try {
      const initializeBody = {
        jsonrpc: "2.0",
        id: 1,
        method: "initialize",
      };
      const initialized = await sendRPC(initializeBody, trimmedToken);
      const nextSessionID = initialized.sessionID;
      if (initialized.status !== 200 || nextSessionID === "") {
        throw new Error(initialized.responseText || "Failed to initialize MCP session.");
      }

      setSessionID(nextSessionID);

      await sendRPC(
        {
          jsonrpc: "2.0",
          method: "notifications/initialized",
        },
        trimmedToken,
        nextSessionID,
      );

      const toolsBody = {
        jsonrpc: "2.0",
        id: 2,
        method: "tools/list",
      };
      const listedTools = await sendRPC(toolsBody, trimmedToken, nextSessionID);
      setToolsResponseText(listedTools.responseText);
      if (listedTools.status !== 200) {
        throw new Error(listedTools.responseText || "Failed to load tool definitions.");
      }

      const payload = listedTools.parsed as
        | { result?: { tools?: McpToolDefinition[] } }
        | null;
      const definitions = payload?.result?.tools ?? [];
      setTools(definitions);
      setToolArguments((previous) => {
        const next = { ...previous };
        for (const tool of definitions) {
          if (!next[tool.name]) {
            next[tool.name] = defaultArgumentsForTool(tool.name);
          }
        }
        return next;
      });
      return nextSessionID;
    } catch (cause) {
      setConnectionError(String(cause));
      throw cause;
    } finally {
      setBusyConnect(false);
    }
  }

  async function executeTool(tool: McpToolDefinition) {
    const trimmedToken = bearerToken.trim();
    if (trimmedToken === "" || sessionID === "") {
      setConnectionError("Finish OAuth and initialize an MCP session before executing tools.");
      return;
    }

    setBusyToolName(tool.name);
    try {
      const argumentsText = toolArguments[tool.name] ?? "{}";
      const argumentsValue = JSON.parse(argumentsText) as unknown;
      const requestBody = {
        jsonrpc: "2.0",
        id: 100,
        method: "tools/call",
        params: {
          name: tool.name,
          arguments: argumentsValue,
        },
      };
      let activeSessionID = sessionID;
      let response = await sendRPC(requestBody, trimmedToken, activeSessionID);
      if (isMissingSession(response)) {
        activeSessionID = await connectWithToken(trimmedToken);
        response = await sendRPC(requestBody, trimmedToken, activeSessionID);
      }
      setToolResponses((previous) => ({
        ...previous,
        [tool.name]: {
          requestBody: prettyJSON(requestBody),
          status: response.status,
          responseText: response.responseText,
        },
      }));
    } catch (cause) {
      setToolResponses((previous) => ({
        ...previous,
        [tool.name]: {
          requestBody: toolArguments[tool.name] ?? "{}",
          status: 0,
          responseText: String(cause),
        },
      }));
    } finally {
      setBusyToolName("");
    }
  }

  async function ensureOAuthClientID(): Promise<string> {
    const stored = readStoredValue(effectiveStorage, oauthClientIDStorageKey).trim();
    if (stored !== "") {
      setOAuthClientID(stored);
      return stored;
    }

    const redirectURI = debugRedirectURI(effectiveLocation);
    const response = await fetchImpl("/oauth/register", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        redirect_uris: [redirectURI],
        client_name: oauthClientName,
      }),
    });
    const registration = await readResponse(response);
    setOAuthResponseText(registration.responseText);
    if (registration.status !== 201) {
      throw new Error(registration.responseText || "Failed to register the debug OAuth client.");
    }

    const payload = registration.parsed as { client_id?: string } | null;
    const clientID = payload?.client_id?.trim() ?? "";
    if (clientID === "") {
      throw new Error("OAuth registration succeeded without returning a client_id.");
    }

    writeStoredValue(effectiveStorage, oauthClientIDStorageKey, clientID);
    setOAuthClientID(clientID);
    return clientID;
  }

  async function beginOAuth() {
    setBusyOAuth(true);
    setOAuthStatus("");
    setConnectionError("");

    try {
      const clientID = await ensureOAuthClientID();
      const verifier = randomHex(32);
      const state = randomHex(16);
      const challenge = await s256Challenge(verifier);
      writeStoredValue(effectiveStorage, oauthVerifierStorageKey, verifier);
      writeStoredValue(effectiveStorage, oauthStateStorageKey, state);

      const authorizeURL = new URL("/oauth/authorize", effectiveLocation.origin);
      authorizeURL.searchParams.set("response_type", "code");
      authorizeURL.searchParams.set("client_id", clientID);
      authorizeURL.searchParams.set("redirect_uri", debugRedirectURI(effectiveLocation));
      authorizeURL.searchParams.set("state", state);
      authorizeURL.searchParams.set("code_challenge", challenge);
      authorizeURL.searchParams.set("code_challenge_method", "S256");

      setOAuthStatus("Redirecting to Airtable OAuth...");
      effectiveLocation.assign(authorizeURL.toString());
    } catch (cause) {
      setConnectionError(String(cause));
      setBusyOAuth(false);
    }
  }

  useEffect(() => {
    const params = new URLSearchParams(effectiveLocation.search);
    const authorizationCode = params.get("code");
    const returnedState = params.get("state");
    const error = params.get("error");
    const errorDescription = params.get("error_description");

    if (!authorizationCode && !error) {
      return;
    }

    let cancelled = false;

    async function finishOAuth() {
      setBusyOAuth(true);
      setConnectionError("");
      setOAuthStatus("");

      try {
        if (error) {
          throw new Error(errorDescription || error);
        }

        const expectedState = readStoredValue(effectiveStorage, oauthStateStorageKey);
        const verifier = readStoredValue(effectiveStorage, oauthVerifierStorageKey);
        const clientID = readStoredValue(effectiveStorage, oauthClientIDStorageKey);

        if (!returnedState || returnedState !== expectedState) {
          throw new Error("OAuth state mismatch while returning to the debugger.");
        }
        if (verifier.trim() === "" || clientID.trim() === "") {
          throw new Error("Missing stored OAuth verifier or client ID for the debugger flow.");
        }

        const body = new URLSearchParams({
          grant_type: "authorization_code",
          client_id: clientID,
          redirect_uri: debugRedirectURI(effectiveLocation),
          code: authorizationCode ?? "",
          code_verifier: verifier,
        });

        const response = await fetchImpl("/oauth/token", {
          method: "POST",
          headers: {
            Accept: "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
          },
          body: body.toString(),
        });
        const tokenResponse = await readResponse(response);
        if (cancelled) {
          return;
        }

        setOAuthResponseText(tokenResponse.responseText);
        if (tokenResponse.status !== 200) {
          throw new Error(tokenResponse.responseText || "Failed to exchange the OAuth authorization code.");
        }

        const payload = tokenResponse.parsed as { access_token?: string } | null;
        const accessToken = payload?.access_token?.trim() ?? "";
        if (accessToken === "") {
          throw new Error("OAuth token exchange succeeded without returning an access token.");
        }

        setBearerToken(accessToken);
        setOAuthStatus("OAuth complete. Bearer token loaded from the provider.");
        clearOAuthFlowState(effectiveStorage);
        effectiveLocation.replace("/debug");
      } catch (cause) {
        if (!cancelled) {
          setConnectionError(String(cause));
          clearOAuthFlowState(effectiveStorage);
          effectiveLocation.replace("/debug");
        }
      } finally {
        if (!cancelled) {
          setBusyOAuth(false);
        }
      }
    }

    void finishOAuth();

    return () => {
      cancelled = true;
    };
  }, [effectiveLocation, effectiveStorage, fetchImpl]);

  return (
    <main className="page-shell">
      <section className="hero-card debug-hero">
        <div className="pill-row debug-pill-row">
          <span className="pill">/debug</span>
          <span className="pill">Endpoint: /mcp</span>
          <span className="pill">Auth: OAuth only</span>
        </div>
        <h1>MCP Tool Debugger</h1>
        <p className="meta-text">
          Start the normal Airtable-backed OAuth flow, mint a bearer token for this server,
          initialize a live MCP session, inspect the tool catalog, and execute tools with
          editable JSON arguments.
        </p>

        <div className="debug-connect-row">
          <div className="debug-panel">
            <label className="debug-label" htmlFor="debug-bearer-token">
              Bearer Token
            </label>
            <textarea
              id="debug-bearer-token"
              aria-label="Bearer Token"
              className="debug-textarea token-textarea"
              placeholder="No token yet. Use Connect with OAuth."
              value={bearerToken}
              readOnly
              spellCheck={false}
            />
          </div>

          <div className="detail-card debug-session-card">
            <h2>Connection</h2>
            <dl className="detail-list">
              <div>
                <dt>OAuth Client ID</dt>
                <dd>{oauthClientID || "not registered yet"}</dd>
              </div>
              <div>
                <dt>Session ID</dt>
                <dd>{sessionID || "not initialized"}</dd>
              </div>
              <div>
                <dt>Loaded Tools</dt>
                <dd>{tools.length}</dd>
              </div>
            </dl>
            <div className="action-row">
              <button className="action-button approve" disabled={busyOAuth} onClick={() => void beginOAuth()}>
                {busyOAuth ? "Connecting..." : "Connect with OAuth"}
              </button>
              <button
                className="action-button approve"
                disabled={busyConnect || bearerToken.trim() === ""}
                onClick={() => {
                  void connectWithToken(bearerToken).catch(() => undefined);
                }}
              >
                {busyConnect ? "Loading..." : "Initialize + Load Tools"}
              </button>
            </div>
            {oauthStatus ? <p className="meta-text debug-status">{oauthStatus}</p> : null}
          </div>
        </div>

        {connectionError ? <p className="error-text">{connectionError}</p> : null}
      </section>

      <section className="preview-card">
        <h2>OAuth Response</h2>
        <pre className="debug-pre">{oauthResponseText || "Connect with OAuth to start the token flow."}</pre>
      </section>

      <section className="preview-card">
        <h2>Tool Catalog Response</h2>
        <pre className="debug-pre">{toolsResponseText || "Connect with OAuth to initialize the tool catalog."}</pre>
      </section>

      <section className="preview-stack">
        {tools.map((tool) => (
          <ToolCard
            key={tool.name}
            tool={tool}
            argumentsText={toolArguments[tool.name] ?? defaultArgumentsForTool(tool.name)}
            onArgumentsChange={(value) =>
              setToolArguments((previous) => ({
                ...previous,
                [tool.name]: value,
              }))
            }
            onExecute={() => void executeTool(tool)}
            busy={busyToolName === tool.name}
            response={toolResponses[tool.name] ?? null}
          />
        ))}
      </section>
    </main>
  );
}
