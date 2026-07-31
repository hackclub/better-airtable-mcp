import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { RecordTable } from "./RecordTable";
import type { LinkedRecordRef, OperationPreview } from "../types";

const LINKED_ID = "recCCCCCCCCCCCCCC";

function makeCreate(overrides: Partial<OperationPreview> = {}): OperationPreview {
  return {
    type: "create_records",
    table: "tasks",
    original_table_name: "Tasks",
    table_id: "tblTasks",
    fields: [
      { name: "Title", type: "singleLineText" },
      { name: "Points", type: "number" },
      { name: "Tags", type: "multipleSelects" },
      { name: "Project", type: "multipleRecordLinks" },
    ],
    records: [
      {
        fields: {
          Title: "Ship the launch email",
          Points: 1234,
          Tags: ["Design", "Email"],
          Project: [LINKED_ID],
        },
      },
      {
        fields: { Title: "Fix onboarding bug", Points: 2 },
      },
    ],
    ...overrides,
  };
}

const linked: Record<string, LinkedRecordRef> = {
  [LINKED_ID]: { name: "Q3 Roadmap", table_id: "tblProjects" },
};

describe("RecordTable", () => {
  it("describes the create operation and links to the table", () => {
    render(<RecordTable operation={makeCreate()} baseId="app123" linked={linked} />);
    expect(screen.getByRole("heading", { name: "Create 2 records in Tasks" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Tasks" })).toHaveAttribute(
      "href",
      "https://airtable.com/app123/tblTasks",
    );
  });

  it("renders a column per schema field with a plain-language type tooltip", () => {
    render(<RecordTable operation={makeCreate()} baseId="app123" linked={linked} />);
    for (const label of ["Title", "Points", "Tags", "Project"]) {
      expect(screen.getByText(label, { selector: "thead th" })).toBeInTheDocument();
    }
    expect(screen.getByText("Tags", { selector: "thead th" })).toHaveAttribute(
      "data-tooltip",
      "Multiple select",
    );
  });

  it("renders each record's values, formatting numbers, tokens and links", () => {
    render(<RecordTable operation={makeCreate()} baseId="app123" linked={linked} />);

    expect(screen.getByText("Ship the launch email")).toBeInTheDocument();
    expect(screen.getByText("Fix onboarding bug")).toBeInTheDocument();
    expect(screen.getByText("1,234")).toBeInTheDocument();

    // Multi-select options render as individual tokens, not one joined string.
    expect(screen.getByText("Design")).toBeInTheDocument();
    expect(screen.getByText("Email")).toBeInTheDocument();

    // Linked record renders as a chip with the resolved primary-field name.
    const link = screen.getByRole("link", { name: "Q3 Roadmap" });
    expect(link).toHaveAttribute("href", `https://airtable.com/app123/tblProjects/${LINKED_ID}`);
  });

  it("marks values a record does not set as Empty", () => {
    render(<RecordTable operation={makeCreate()} baseId="app123" linked={linked} />);
    // Second record sets neither Tags nor Project.
    expect(screen.getAllByText("Empty")).toHaveLength(2);
  });

  it("always shows the primary column and hides schema fields no record sets", () => {
    const operation = makeCreate({
      fields: [
        { name: "Name", type: "singleLineText" },
        { name: "Notes", type: "multilineText" },
      ],
      records: [{ fields: {} }],
    });
    render(<RecordTable operation={operation} baseId="app123" />);
    expect(screen.getByText("Name", { selector: "thead th" })).toBeInTheDocument();
    expect(screen.queryByText("Notes")).toBeNull();
  });

  it("falls back to sorted record keys when there is no schema", () => {
    const operation = makeCreate({
      fields: undefined,
      table_id: undefined,
      records: [{ fields: { beta: 7 } }, { fields: { alpha: "First" } }],
    });
    const { container } = render(<RecordTable operation={operation} />);
    const headers = Array.from(container.querySelectorAll("thead th")).map(
      (th) => th.textContent,
    );
    expect(headers).toEqual(["alpha", "beta"]);
    expect(screen.getByText("First")).toBeInTheDocument();
    expect(screen.getByText("7")).toBeInTheDocument();
    // Without a table id the table name is plain text, not a link.
    expect(screen.queryByRole("link", { name: "Tasks" })).toBeNull();
  });
});
