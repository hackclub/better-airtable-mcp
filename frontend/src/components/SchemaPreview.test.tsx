import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { SchemaOperationList, SchemaPreview } from "./SchemaPreview";
import type { SchemaOperationPreview } from "../types";

describe("SchemaPreview", () => {
  it("describes creating a table and lists its fields with plain-language types", () => {
    const operation: SchemaOperationPreview = {
      type: "create_table",
      table_name: "Tasks",
      fields: [
        { name: "Title", type: "singleLineText" },
        { name: "Priority", type: "singleSelect", choices: ["High", "Medium", "Low"] },
      ],
    };
    render(<SchemaPreview operation={operation} baseId="app123" />);

    expect(screen.getByRole("heading", { name: "Create table Tasks" })).toBeInTheDocument();

    // Field list: names, human type labels (no raw enums), primary marker, choices.
    expect(screen.getByText("Title")).toBeInTheDocument();
    expect(screen.getByText("Single line text")).toBeInTheDocument();
    expect(screen.getByText("Primary")).toBeInTheDocument();
    expect(screen.getByText("Priority")).toBeInTheDocument();
    expect(screen.getByText("Single select")).toBeInTheDocument();
    expect(screen.queryByText("singleLineText")).toBeNull();
    for (const choice of ["High", "Medium", "Low"]) {
      expect(screen.getByText(choice)).toBeInTheDocument();
    }
  });

  it("describes adding a field with its type, options and description", () => {
    const operation: SchemaOperationPreview = {
      type: "create_field",
      table_id: "tblTasks",
      table_name: "Tasks",
      field_name: "Priority",
      field_type: "singleSelect",
      choices: ["High", "Low"],
      description: "How urgent this task is",
    };
    render(<SchemaPreview operation={operation} baseId="app123" />);

    expect(screen.getByRole("heading", { name: "Add field Priority to Tasks" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Tasks" })).toHaveAttribute(
      "href",
      "https://airtable.com/app123/tblTasks",
    );
    expect(screen.getByText("Single select")).toBeInTheDocument();
    expect(screen.getByText("High")).toBeInTheDocument();
    expect(screen.getByText("Low")).toBeInTheDocument();
    expect(screen.getByText("How urgent this task is")).toBeInTheDocument();
  });

  it("describes a table rename as a sentence naming both names", () => {
    const operation: SchemaOperationPreview = {
      type: "update_table",
      table_id: "tblTasks",
      table_name: "Tasks",
      new_name: "Projects",
    };
    render(<SchemaPreview operation={operation} baseId="app123" />);
    expect(
      screen.getByRole("heading", { name: "Rename table Tasks to Projects" }),
    ).toBeInTheDocument();
  });

  it("describes a field rename with old and new names in the heading", () => {
    const operation: SchemaOperationPreview = {
      type: "update_field",
      table_id: "tblTasks",
      table_name: "Tasks",
      field_name: "Status",
      new_name: "Stage",
    };
    render(<SchemaPreview operation={operation} baseId="app123" />);
    expect(
      screen.getByRole("heading", { name: "Rename field Status to Stage in Tasks" }),
    ).toBeInTheDocument();
  });

  it("shows a description change with the old value marked as removed", () => {
    const operation: SchemaOperationPreview = {
      type: "update_field",
      table_id: "tblTasks",
      table_name: "Tasks",
      field_name: "Status",
      description: "Where this task is in the flow",
      old_description: "Task status",
    };
    render(<SchemaPreview operation={operation} baseId="app123" />);

    expect(
      screen.getByRole("heading", { name: "Update field Status in Tasks" }),
    ).toBeInTheDocument();
    expect(screen.getByText("Description")).toBeInTheDocument();
    expect(screen.getByText("Task status")).toHaveClass("is-removed");
    expect(screen.getByText("Where this task is in the flow")).toHaveClass("is-added");
  });

  it("shows 'empty' as the previous value when a description is first set", () => {
    const operation: SchemaOperationPreview = {
      type: "update_table",
      table_id: "tblTasks",
      table_name: "Tasks",
      description: "All engineering work",
    };
    render(<SchemaPreview operation={operation} baseId="app123" />);
    expect(screen.getByRole("heading", { name: "Update table Tasks" })).toBeInTheDocument();
    expect(screen.getByText("empty")).toBeInTheDocument();
    expect(screen.getByText("All engineering work")).toHaveClass("is-added");
  });
});

describe("SchemaOperationList", () => {
  it("groups several new fields on one table into a single card", () => {
    const operations: SchemaOperationPreview[] = [
      {
        type: "create_field",
        table_id: "tblTasks",
        table_name: "Tasks",
        field_name: "Due",
        field_type: "date",
      },
      {
        type: "create_field",
        table_id: "tblTasks",
        table_name: "Tasks",
        field_name: "Done",
        field_type: "checkbox",
      },
    ];
    render(<SchemaOperationList operations={operations} baseId="app123" />);

    expect(screen.getByRole("heading", { name: "Add 2 fields to Tasks" })).toBeInTheDocument();
    expect(screen.getByText("Due")).toBeInTheDocument();
    expect(screen.getByText("Date")).toBeInTheDocument();
    expect(screen.getByText("Done")).toBeInTheDocument();
    expect(screen.getByText("Checkbox")).toBeInTheDocument();
  });

  it("keeps a lone field addition as its own richer card", () => {
    const operations: SchemaOperationPreview[] = [
      {
        type: "create_field",
        table_id: "tblTasks",
        table_name: "Tasks",
        field_name: "Due",
        field_type: "date",
      },
      {
        type: "create_field",
        table_id: "tblDocs",
        table_name: "Docs",
        field_name: "Owner",
        field_type: "singleLineText",
      },
    ];
    render(<SchemaOperationList operations={operations} baseId="app123" />);

    // Different tables: no grouping, each keeps its sentence heading.
    expect(screen.getByRole("heading", { name: "Add field Due to Tasks" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Add field Owner to Docs" })).toBeInTheDocument();
  });

  it("groups several field renames on one table, showing each old and new name", () => {
    const operations: SchemaOperationPreview[] = [
      {
        type: "update_field",
        table_id: "tblTasks",
        table_name: "Tasks",
        field_name: "Status",
        new_name: "Stage",
      },
      {
        type: "update_field",
        table_id: "tblTasks",
        table_name: "Tasks",
        field_name: "Due",
        new_name: "Due date",
      },
    ];
    render(<SchemaOperationList operations={operations} baseId="app123" />);

    expect(screen.getByRole("heading", { name: "Rename 2 fields in Tasks" })).toBeInTheDocument();
    expect(screen.getByText("Status")).toHaveClass("is-removed");
    expect(screen.getByText("Stage")).toHaveClass("is-added");
    expect(screen.getByText("Due")).toHaveClass("is-removed");
    expect(screen.getByText("Due date")).toHaveClass("is-added");
  });
});
