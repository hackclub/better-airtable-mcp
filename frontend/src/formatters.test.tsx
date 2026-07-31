import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import {
  absoluteTime,
  baseUrl,
  buildColumns,
  choiceColor,
  collectFieldNames,
  countdownLabel,
  fieldTypeIcon,
  fieldTypeLabel,
  formatFieldValue,
  getOperationIDFromPath,
  inferColumnType,
  inferFieldType,
  orderFieldKeys,
  primaryFieldDisplay,
  recordCountLabel,
  recordUrl,
  relativeTime,
  statusLabel,
  statusTone,
  tableUrl,
  valueToText,
} from "./formatters";
import type { FieldType, FieldContext } from "./formatters";
import type { FieldMeta, OperationStatus } from "./types";

// Valid Airtable record ids: "rec" + >=14 alphanumerics.
const LINKED_ID = "recAAAAAAAAAAAAAA";
const OTHER_LINKED_ID = "recBBBBBBBBBBBBBB";

describe("formatters", () => {
  it("extracts the operation ID from the approval path", () => {
    expect(getOperationIDFromPath("/approve/op_123")).toBe("op_123");
    expect(getOperationIDFromPath("/")).toBe("");
  });

  it("builds a stable countdown label", () => {
    expect(
      countdownLabel("2026-04-01T12:10:05Z", new Date("2026-04-01T12:09:00Z")),
    ).toBe("1m 05s");
    expect(
      countdownLabel("2026-04-01T12:09:00Z", new Date("2026-04-01T12:09:00Z")),
    ).toBe("expired");
  });

  it("renders a bare date as the same calendar day in timezones west of UTC", () => {
    // Tests run with TZ=America/New_York (see the test script); a UTC-midnight
    // parse would render 2026-07-24 as the 23rd.
    expect(formatFieldValue("2026-07-24")).toContain("24");
  });

  it("collects sorted unique field names from current and requested values", () => {
    expect(
      collectFieldNames(
        { status: "Planning", owner: "Ava" },
        { status: "Done", name: "Website" },
      ),
    ).toEqual(["name", "owner", "status"]);
    expect(collectFieldNames(undefined, undefined)).toEqual([]);
  });
});

describe("inferFieldType", () => {
  const cases: { name: string; input: unknown; expected: FieldType }[] = [
    { name: "boolean", input: true, expected: "checkbox" },
    { name: "number", input: 3.14, expected: "number" },
    { name: "url string", input: "https://example.com/page", expected: "url" },
    { name: "email string", input: "ava@example.com", expected: "email" },
    { name: "bare date string", input: "2026-07-24", expected: "date" },
    { name: "ISO datetime string", input: "2026-04-01T12:00:00Z", expected: "date" },
    { name: "plain string", input: "Website Redesign", expected: "text" },
    { name: "null", input: null, expected: "text" },
    { name: "undefined", input: undefined, expected: "text" },
    { name: "plain object", input: { a: 1 }, expected: "text" },
    { name: "string array (multi select)", input: ["Red", "Green"], expected: "multiSelect" },
    { name: "record-id array", input: [LINKED_ID, OTHER_LINKED_ID], expected: "linkedRecord" },
    {
      name: "object array (attachments)",
      input: [{ filename: "a.pdf", url: "https://example.com/a.pdf" }],
      expected: "attachment",
    },
  ];

  for (const { name, input, expected } of cases) {
    it(`classifies ${name} as ${expected}`, () => {
      expect(inferFieldType(input)).toBe(expected);
    });
  }
});

describe("inferColumnType", () => {
  it("uses the first non-empty value in the column", () => {
    expect(inferColumnType([null, undefined, "", [], "ava@example.com"])).toBe("email");
    expect(inferColumnType(["", 12])).toBe("number");
  });

  it("falls back to text when every value is empty", () => {
    expect(inferColumnType([null, undefined, "", []])).toBe("text");
    expect(inferColumnType([])).toBe("text");
  });
});

describe("fieldTypeIcon", () => {
  const cases: [string, FieldType][] = [
    ["singleLineText", "text"],
    ["multipleSelects", "multiSelect"],
    ["multipleRecordLinks", "linkedRecord"],
    ["multipleAttachments", "attachment"],
    ["dateTime", "date"],
    ["checkbox", "checkbox"],
    ["someBrandNewType", "text"], // unknown types degrade to text
  ];

  for (const [atType, expected] of cases) {
    it(`maps ${atType} to ${expected}`, () => {
      expect(fieldTypeIcon(atType)).toBe(expected);
    });
  }
});

describe("fieldTypeLabel", () => {
  it("prefers the precise Airtable type name", () => {
    expect(fieldTypeLabel("multipleSelects", "text")).toBe("Multiple select");
    expect(fieldTypeLabel("multipleRecordLinks", "text")).toBe("Linked records");
  });

  it("falls back to the inferred field-type label without a schema type", () => {
    expect(fieldTypeLabel(undefined, "linkedRecord")).toBe("Linked records");
    expect(fieldTypeLabel("unknownAtType", "currency")).toBe("Currency");
  });
});

describe("choiceColor", () => {
  it("cycles Airtable's default select palette", () => {
    expect(choiceColor(0)).toMatch(/^#[0-9a-f]{6}$/i);
    expect(choiceColor(10)).toBe(choiceColor(0));
    expect(choiceColor(23)).toBe(choiceColor(3));
  });
});

describe("recordCountLabel", () => {
  it("pluralizes record counts", () => {
    expect(recordCountLabel(0)).toBe("0 records");
    expect(recordCountLabel(1)).toBe("1 record");
    expect(recordCountLabel(5)).toBe("5 records");
  });
});

describe("Airtable URL builders", () => {
  it("builds base, table and record URLs when every id is present", () => {
    expect(baseUrl("app1")).toBe("https://airtable.com/app1");
    expect(tableUrl("app1", "tbl1")).toBe("https://airtable.com/app1/tbl1");
    expect(recordUrl("app1", "tbl1", "rec1")).toBe("https://airtable.com/app1/tbl1/rec1");
  });

  it("returns null when any id is missing", () => {
    expect(baseUrl(undefined)).toBeNull();
    expect(tableUrl("app1", undefined)).toBeNull();
    expect(tableUrl(undefined, "tbl1")).toBeNull();
    expect(recordUrl("app1", "tbl1", undefined)).toBeNull();
    expect(recordUrl("app1", undefined, "rec1")).toBeNull();
  });
});

describe("orderFieldKeys", () => {
  const fields: FieldMeta[] = [
    { name: "Name", type: "singleLineText" },
    { name: "Status", key: "fldStatus", type: "singleSelect" },
  ];

  it("orders keys by schema position with schema labels and types", () => {
    const ordered = orderFieldKeys(["zeta", "Status", "Name"], fields);
    expect(ordered.map((f) => f.key)).toEqual(["Name", "Status", "zeta"]);
    expect(ordered[0]).toMatchObject({ label: "Name", type: "text", atType: "singleLineText" });
    expect(ordered[1]).toMatchObject({ label: "Status", type: "singleSelect" });
    // Unknown key falls back to its own name and an inferred type.
    expect(ordered[2]).toMatchObject({ label: "zeta", type: "text" });
  });

  it("resolves a field id key to the schema display name", () => {
    const ordered = orderFieldKeys(["fldStatus"], fields);
    expect(ordered).toHaveLength(1);
    expect(ordered[0]).toMatchObject({ key: "fldStatus", label: "Status", type: "singleSelect" });
  });

  it("sorts keys alphabetically and infers types when there is no schema", () => {
    const ordered = orderFieldKeys(["b", "a"], undefined, (key) => (key === "a" ? 5 : "x"));
    expect(ordered.map((f) => f.key)).toEqual(["a", "b"]);
    expect(ordered[0].type).toBe("number");
    expect(ordered[1].type).toBe("text");
  });
});

describe("buildColumns", () => {
  const fields: FieldMeta[] = [
    { name: "Name", type: "singleLineText" },
    { name: "Status", key: "fldStatus", type: "singleSelect" },
    { name: "Notes", type: "multilineText" },
  ];

  it("keeps schema order, always includes the primary field, and hides absent fields", () => {
    const columns = buildColumns(fields, [{ fields: { Status: "Todo" } }]);
    // Name (primary) always shows even though no record sets it; Notes is
    // hidden because no record touches it.
    expect(columns.map((c) => c.label)).toEqual(["Name", "Status"]);
    expect(columns[1].atType).toBe("singleSelect");
  });

  it("resolves values set by field id through the schema key", () => {
    const columns = buildColumns(fields, [{ fields: { fldStatus: "Todo" } }]);
    const status = columns.find((c) => c.label === "Status");
    expect(status).toBeDefined();
    expect(status?.get({ fldStatus: "Todo" })).toBe("Todo");
  });

  it("appends unknown keys after schema fields with inferred types", () => {
    const columns = buildColumns(fields, [{ fields: { Name: "A", zzz: 12 } }]);
    expect(columns.map((c) => c.label)).toEqual(["Name", "zzz"]);
    expect(columns[1].type).toBe("number");
  });

  it("falls back to the sorted union of record keys without a schema", () => {
    const columns = buildColumns(undefined, [
      { fields: { beta: 2 } },
      { fields: { alpha: "x" } },
    ]);
    expect(columns.map((c) => c.label)).toEqual(["alpha", "beta"]);
    expect(columns[0].type).toBe("text");
    expect(columns[1].type).toBe("number");
  });
});

describe("primaryFieldDisplay", () => {
  const fields: FieldMeta[] = [
    { name: "Name", key: "fldName", type: "singleLineText" },
    { name: "Status", type: "singleSelect" },
  ];

  it("prefers the requested value over the current one", () => {
    expect(
      primaryFieldDisplay(
        { current_fields: { Name: "Old title" }, fields: { Name: "New title" } },
        fields,
      ),
    ).toBe("New title");
  });

  it("uses the current value when the update does not touch the primary field", () => {
    expect(
      primaryFieldDisplay({ current_fields: { Name: "Website" }, fields: { Status: "Done" } }, fields),
    ).toBe("Website");
  });

  it("resolves the primary value set by field id", () => {
    expect(primaryFieldDisplay({ fields: { fldName: "Via key" } }, fields)).toBe("Via key");
  });

  it("returns null for empty primaries or a missing schema", () => {
    expect(primaryFieldDisplay({ fields: { Name: "" } }, fields)).toBeNull();
    expect(primaryFieldDisplay({ fields: { Status: "Done" } }, fields)).toBeNull();
    expect(primaryFieldDisplay({ fields: { Name: "Website" } }, undefined)).toBeNull();
    expect(primaryFieldDisplay({ fields: { Name: "Website" } }, [])).toBeNull();
  });
});

describe("relativeTime", () => {
  const now = new Date("2026-04-01T12:00:00Z");

  it("describes recent and upcoming times in plain language", () => {
    expect(relativeTime("2026-04-01T11:59:30Z", now)).toBe("just now");
    expect(relativeTime("2026-04-01T11:55:00Z", now)).toBe("5 min ago");
    expect(relativeTime("2026-04-01T12:05:00Z", now)).toBe("in 5 min");
    expect(relativeTime("2026-04-01T09:00:00Z", now)).toBe("3 hr ago");
  });

  it("falls back to a calendar date beyond a day", () => {
    expect(relativeTime("2026-03-25T12:00:00Z", now)).toBe(
      new Date("2026-03-25T12:00:00Z").toLocaleDateString(),
    );
  });

  it("returns unknown for an unparseable timestamp", () => {
    expect(relativeTime("not a date", now)).toBe("unknown");
  });
});

describe("absoluteTime", () => {
  it("renders a medium date with local time (TZ=America/New_York)", () => {
    const text = absoluteTime("2026-04-01T12:00:00Z");
    expect(text).toContain("2026");
    expect(text).toContain("8:00"); // 12:00 UTC is 8:00 AM in New York (EDT)
  });

  it("returns an empty string for invalid input", () => {
    expect(absoluteTime("not a date")).toBe("");
  });
});

describe("status presentation", () => {
  const cases: [OperationStatus, string, string][] = [
    ["pending_approval", "Waiting for your approval", "info"],
    ["approved", "Approved", "info"],
    ["rejected", "Rejected", "danger"],
    ["expired", "Request expired", "neutral"],
    ["executing", "Applying changes", "info"],
    ["completed", "Changes applied", "success"],
    ["partially_completed", "Partly applied", "warning"],
    ["failed", "Couldn't apply changes", "danger"],
  ];

  for (const [status, label, tone] of cases) {
    it(`labels ${status} as "${label}" with a ${tone} tone`, () => {
      expect(statusLabel(status)).toBe(label);
      expect(statusTone(status)).toBe(tone);
    });
  }
});

describe("formatFieldValue", () => {
  const emptyCases: { name: string; input: unknown }[] = [
    { name: "null", input: null },
    { name: "undefined", input: undefined },
    { name: "empty string", input: "" },
    { name: "empty array", input: [] },
  ];

  for (const { name, input } of emptyCases) {
    it(`renders ${name} as Empty`, () => {
      render(<div>{formatFieldValue(input)}</div>);
      expect(screen.getByText("Empty")).toBeInTheDocument();
    });
  }

  it("renders booleans as checked and unchecked checkboxes", () => {
    const { unmount } = render(<div>{formatFieldValue(true)}</div>);
    expect(screen.getByText("Checked")).toBeInTheDocument();
    unmount();
    render(<div>{formatFieldValue(false)}</div>);
    expect(screen.getByText("Unchecked")).toBeInTheDocument();
  });

  it("renders numbers with thousands separators", () => {
    render(<div>{formatFieldValue(1234567)}</div>);
    expect(screen.getByText("1,234,567")).toBeInTheDocument();
  });

  it("renders a datetime in the local timezone, not raw UTC", () => {
    const { container } = render(<div>{formatFieldValue("2026-04-01T12:00:00Z")}</div>);
    expect(container.textContent).toContain("8:00"); // America/New_York
    expect(container.textContent).not.toContain("12:00");
  });

  it("does not shift a bare date to the previous day", () => {
    const { container } = render(<div>{formatFieldValue("2026-07-24")}</div>);
    expect(container.textContent).toContain("24");
    expect(container.textContent).not.toContain("23");
  });

  it("renders URLs as external links", () => {
    render(<div>{formatFieldValue("https://example.com/spec")}</div>);
    const link = screen.getByRole("link", { name: "https://example.com/spec" });
    expect(link).toHaveAttribute("href", "https://example.com/spec");
  });

  it("renders email addresses as mailto links", () => {
    render(<div>{formatFieldValue("ava@example.com")}</div>);
    const link = screen.getByRole("link", { name: "ava@example.com" });
    expect(link).toHaveAttribute("href", "mailto:ava@example.com");
  });

  it("renders each multi-select option as its own token", () => {
    render(<div>{formatFieldValue(["Design", "Engineering", "Marketing"])}</div>);
    // Exact-match lookups fail if the options were joined into one string.
    expect(screen.getByText("Design")).toBeInTheDocument();
    expect(screen.getByText("Engineering")).toBeInTheDocument();
    expect(screen.getByText("Marketing")).toBeInTheDocument();
  });

  it("resolves linked records to their primary-field name with a record link", () => {
    const ctx: FieldContext = {
      baseId: "app123",
      linked: { [LINKED_ID]: { name: "Q3 Roadmap", table_id: "tblProjects" } },
    };
    render(<div>{formatFieldValue([LINKED_ID], ctx)}</div>);
    const link = screen.getByRole("link", { name: "Q3 Roadmap" });
    expect(link).toHaveAttribute("href", `https://airtable.com/app123/tblProjects/${LINKED_ID}`);
  });

  it("falls back to the raw record id when a linked record is unresolved", () => {
    render(<div>{formatFieldValue([LINKED_ID])}</div>);
    expect(screen.getByText(LINKED_ID)).toBeInTheDocument();
    expect(screen.queryByRole("link")).toBeNull();
  });

  it("shows a resolved linked-record name without a link when the base is unknown", () => {
    const ctx: FieldContext = { linked: { [LINKED_ID]: { name: "Q3 Roadmap" } } };
    render(<div>{formatFieldValue([LINKED_ID], ctx)}</div>);
    expect(screen.getByText("Q3 Roadmap")).toBeInTheDocument();
    expect(screen.queryByRole("link")).toBeNull();
  });

  it("renders a non-image attachment as a filename link", () => {
    render(
      <div>
        {formatFieldValue([{ filename: "report.pdf", url: "https://example.com/report.pdf" }])}
      </div>,
    );
    const link = screen.getByRole("link", { name: "report.pdf" });
    expect(link).toHaveAttribute("href", "https://example.com/report.pdf");
  });

  it("renders an image attachment as a thumbnail linking to the full file", () => {
    render(
      <div>
        {formatFieldValue([
          {
            filename: "photo.png",
            url: "https://example.com/photo.png",
            type: "image/png",
            thumbnails: { large: { url: "https://example.com/thumb.png", width: 100, height: 50 } },
          },
        ])}
      </div>,
    );
    const img = screen.getByAltText("photo.png");
    expect(img).toHaveAttribute("src", "https://example.com/thumb.png");
    expect(img.closest("a")).toHaveAttribute("href", "https://example.com/photo.png");
  });

  it("labels an attachment without a URL by filename or a generic tag", () => {
    const { unmount } = render(<div>{formatFieldValue([{ filename: "notes.txt" }])}</div>);
    expect(screen.getByText("notes.txt")).toBeInTheDocument();
    expect(screen.queryByRole("link")).toBeNull();
    unmount();
    render(<div>{formatFieldValue([{}])}</div>);
    expect(screen.getByText("Attachment")).toBeInTheDocument();
  });

  it("renders an unrecognized object as Object", () => {
    render(<div>{formatFieldValue({ nested: true })}</div>);
    expect(screen.getByText("Object")).toBeInTheDocument();
  });
});

describe("valueToText", () => {
  const cases: { name: string; input: unknown; expected: string }[] = [
    { name: "null", input: null, expected: "Empty" },
    { name: "empty string", input: "", expected: "Empty" },
    { name: "true", input: true, expected: "Checked" },
    { name: "false", input: false, expected: "Unchecked" },
    { name: "number", input: 1234, expected: "1,234" },
    { name: "plain string", input: "hello", expected: "hello" },
    { name: "attachment array", input: [{ filename: "a.pdf" }, {}], expected: "a.pdf, Attachment" },
    { name: "plain object", input: { a: 1 }, expected: '{"a":1}' },
  ];

  for (const { name, input, expected } of cases) {
    it(`converts ${name} to "${expected}"`, () => {
      expect(valueToText(input)).toBe(expected);
    });
  }

  it("joins arrays and resolves linked-record names", () => {
    const ctx: FieldContext = { linked: { [LINKED_ID]: { name: "Alice" } } };
    expect(valueToText([LINKED_ID, "Tag"], ctx)).toBe("Alice, Tag");
    expect(valueToText([LINKED_ID, "Tag"])).toBe(`${LINKED_ID}, Tag`);
  });
});
