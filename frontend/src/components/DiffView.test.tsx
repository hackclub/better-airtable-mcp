import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { DiffView } from "./DiffView";
import type { LinkedRecordRef, OperationPreview } from "../types";

const RECORD_ID = "recAAAAAAAAAAAAAA";
const LINKED_ID = "recBBBBBBBBBBBBBB";

function makeUpdate(overrides: Partial<OperationPreview> = {}): OperationPreview {
  return {
    type: "update_records",
    table: "projects",
    original_table_name: "Projects",
    table_id: "tblProjects",
    fields: [
      { name: "Name", type: "singleLineText" },
      { name: "Status", type: "singleSelect" },
      { name: "Owner", type: "singleLineText" },
    ],
    records: [
      {
        id: RECORD_ID,
        current_fields: { Name: "Website Redesign", Status: "Planning", Owner: "Ava" },
        fields: { Name: "Website Redesign", Status: "Done" },
      },
    ],
    ...overrides,
  };
}

describe("DiffView", () => {
  it("describes the update and links to the table", () => {
    render(<DiffView operation={makeUpdate()} baseId="app123" />);
    const heading = screen.getByRole("heading", { name: "Update 1 record in Projects" });
    expect(heading).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Projects" })).toHaveAttribute(
      "href",
      "https://airtable.com/app123/tblProjects",
    );
  });

  it("shows the old value under Was and the new value under Now for a changed field", () => {
    const { container } = render(<DiffView operation={makeUpdate()} baseId="app123" />);

    const headerCells = Array.from(container.querySelectorAll("thead th")).map(
      (th) => th.textContent,
    );
    expect(headerCells).toEqual(["Field", "Was", "Now"]);

    const fieldCell = screen.getByText("Status", { selector: "tbody th" });
    const row = fieldCell.closest("tr");
    expect(row).not.toBeNull();
    const [wasCell, nowCell] = Array.from(row!.querySelectorAll("td"));
    expect(wasCell).toHaveTextContent("Planning");
    expect(nowCell).toHaveTextContent("Done");
  });

  it("folds unchanged fields away and summarizes them in a note", () => {
    render(<DiffView operation={makeUpdate()} baseId="app123" />);

    // Name is requested but identical, Owner exists only in current_fields:
    // neither gets a diff row. Name still appears once as the record title.
    expect(screen.queryByText("Ava")).toBeNull();
    const titleLink = screen.getByRole("link", { name: "Website Redesign" });
    expect(titleLink).toHaveAttribute(
      "href",
      `https://airtable.com/app123/tblProjects/${RECORD_ID}`,
    );
    expect(screen.getAllByText("Website Redesign")).toHaveLength(1);

    const hint = screen.getByText("2 other fields");
    expect(hint.parentElement).toHaveTextContent("2 other fields unchanged");
    expect(hint).toHaveAttribute("data-tooltip", "Name, Owner");
  });

  it("truncates a long unchanged-fields tooltip after 12 names", () => {
    const current: Record<string, unknown> = { Status: "Planning" };
    const next: Record<string, unknown> = { Status: "Done" };
    for (let i = 1; i <= 15; i += 1) {
      const key = `extra_${String(i).padStart(2, "0")}`;
      current[key] = "same";
      next[key] = "same";
    }
    const operation = makeUpdate({
      records: [{ id: RECORD_ID, current_fields: current, fields: next }],
    });

    render(<DiffView operation={operation} baseId="app123" />);

    const hint = screen.getByText("15 other fields");
    const tooltip = hint.getAttribute("data-tooltip") ?? "";
    expect(tooltip).toContain("extra_01");
    expect(tooltip).toContain("extra_12");
    expect(tooltip).not.toContain("extra_13");
    expect(tooltip.endsWith(", and 3 more")).toBe(true);
  });

  it("says so explicitly when a record has no effective changes", () => {
    const operation = makeUpdate({
      records: [
        {
          id: RECORD_ID,
          current_fields: { Name: "Website Redesign" },
          fields: { Name: "Website Redesign" },
        },
      ],
    });
    render(<DiffView operation={operation} baseId="app123" />);
    expect(screen.getByText("No field values change for this record.")).toBeInTheDocument();
  });

  it("shows the record id and an Untitled title when there is no primary value", () => {
    const operation = makeUpdate({
      fields: undefined,
      records: [
        {
          id: RECORD_ID,
          current_fields: { Status: "Planning" },
          fields: { Status: "Done" },
        },
      ],
    });
    render(<DiffView operation={operation} baseId="app123" />);
    expect(screen.getByText(RECORD_ID)).toBeInTheDocument();
    expect(screen.getByText("Untitled record")).toBeInTheDocument();
  });

  it("resolves linked-record values in the diff cells", () => {
    const linked: Record<string, LinkedRecordRef> = {
      [LINKED_ID]: { name: "Q3 Roadmap", table_id: "tblDocs" },
    };
    const operation = makeUpdate({
      records: [
        {
          id: RECORD_ID,
          current_fields: { Name: "Website Redesign", Project: [] },
          fields: { Project: [LINKED_ID] },
        },
      ],
    });
    render(<DiffView operation={operation} baseId="app123" linked={linked} />);
    const link = screen.getByRole("link", { name: "Q3 Roadmap" });
    expect(link).toHaveAttribute("href", `https://airtable.com/app123/tblDocs/${LINKED_ID}`);
  });

  it("renders a diff card per record", () => {
    const operation = makeUpdate({
      records: [
        {
          id: RECORD_ID,
          current_fields: { Name: "Website Redesign", Status: "Planning" },
          fields: { Status: "Done" },
        },
        {
          id: LINKED_ID,
          current_fields: { Name: "Mobile App", Status: "Backlog" },
          fields: { Status: "In Progress" },
        },
      ],
    });
    render(<DiffView operation={operation} baseId="app123" />);
    expect(screen.getByRole("heading", { name: "Update 2 records in Projects" })).toBeInTheDocument();
    expect(screen.getByText("Planning")).toBeInTheDocument();
    expect(screen.getByText("Done")).toBeInTheDocument();
    expect(screen.getByText("Backlog")).toBeInTheDocument();
    expect(screen.getByText("In Progress")).toBeInTheDocument();
  });
});
