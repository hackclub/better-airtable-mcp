import { describe, expect, it } from "vitest";
import {
  collectFieldNames,
  countdownLabel,
  getOperationIDFromPath,
} from "./formatters";

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

  it("collects sorted unique field names from current and requested values", () => {
    expect(
      collectFieldNames(
        { status: "Planning", owner: "Ava" },
        { status: "Done", name: "Website" },
      ),
    ).toEqual(["name", "owner", "status"]);
  });
});
