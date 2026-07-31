import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { DeletePreview } from "./DeletePreview";
import type { OperationPreview } from "../types";

const RECORD_ID = "recAAAAAAAAAAAAAA";
const SECOND_ID = "recBBBBBBBBBBBBBB";

function makeDelete(overrides: Partial<OperationPreview> = {}): OperationPreview {
  return {
    type: "delete_records",
    table: "projects",
    original_table_name: "Projects",
    table_id: "tblProjects",
    fields: [
      { name: "Name", type: "singleLineText" },
      { name: "Status", type: "singleSelect" },
      { name: "Budget", type: "number" },
    ],
    records: [
      {
        id: RECORD_ID,
        current_fields: { Name: "Website Redesign", Status: "Planning", Budget: 12000 },
      },
      {
        id: SECOND_ID,
        current_fields: { Status: "Backlog" },
      },
    ],
    ...overrides,
  };
}

describe("DeletePreview", () => {
  it("states how many records are deleted and from which table", () => {
    render(<DeletePreview operation={makeDelete()} baseId="app123" />);
    expect(
      screen.getByRole("heading", { name: "Delete 2 records from Projects" }),
    ).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Projects" })).toHaveAttribute(
      "href",
      "https://airtable.com/app123/tblProjects",
    );
  });

  it("shows every current field value of each record being deleted", () => {
    render(<DeletePreview operation={makeDelete()} baseId="app123" />);

    // One "Current value" column per record card.
    expect(screen.getAllByText("Current value")).toHaveLength(2);

    // First record: all three fields with their values.
    expect(screen.getByText("Planning")).toBeInTheDocument();
    expect(screen.getByText("12,000")).toBeInTheDocument();
    // "Website Redesign" appears both as the record title and in the Name row.
    expect(screen.getAllByText("Website Redesign").length).toBeGreaterThanOrEqual(2);

    // Second record's value.
    expect(screen.getByText("Backlog")).toBeInTheDocument();
  });

  it("titles each record with its primary field, linking to the record", () => {
    render(<DeletePreview operation={makeDelete()} baseId="app123" />);
    const titleLink = screen.getByRole("link", { name: "Website Redesign" });
    expect(titleLink).toHaveAttribute(
      "href",
      `https://airtable.com/app123/tblProjects/${RECORD_ID}`,
    );
    // The second record has no primary value, so it reads Untitled.
    expect(screen.getByText("Untitled record")).toBeInTheDocument();
  });

  it("shows each record id so the reviewer can verify what is removed", () => {
    render(<DeletePreview operation={makeDelete()} baseId="app123" />);
    expect(screen.getByText(RECORD_ID)).toBeInTheDocument();
    expect(screen.getByText(SECOND_ID)).toBeInTheDocument();
  });

  it("orders and labels fields via the schema, singular for one record", () => {
    const operation = makeDelete({
      records: [
        {
          id: RECORD_ID,
          current_fields: { fldB: "by-key" },
        },
      ],
      fields: [
        { name: "Name", type: "singleLineText" },
        { name: "Bravo", key: "fldB", type: "singleLineText" },
      ],
    });
    render(<DeletePreview operation={operation} baseId="app123" />);
    expect(
      screen.getByRole("heading", { name: "Delete 1 record from Projects" }),
    ).toBeInTheDocument();
    // The field id key is shown under its schema display name.
    expect(screen.getByText("Bravo")).toBeInTheDocument();
    expect(screen.getByText("by-key")).toBeInTheDocument();
  });
});
