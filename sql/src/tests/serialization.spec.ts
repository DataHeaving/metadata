import test from "ava";
import * as spec from "../serialization";

test("Previously existing table does not affect result when latest is array without that table", (t) => {
  const previousMetaData: spec.ComparisonOptions<string>["previousMetaData"] = {
    table: {
      mdVersion: 0,
      columns: {
        col: {
          typeName: "INT",
          isNullable: false,
          isPrimaryKey: true,
          maxLength: 0,
          precision: 0,
          scale: 0,
        },
      },
    },
    anotherTable: {
      mdVersion: 0,
      columns: {},
    },
  };
  const latestMetaData: spec.ComparisonOptions<string>["latestMetaData"] = [
    {
      tableID: "table",
      tableMD: {
        columnNames: ["col"],
        columnTypes: [
          {
            typeName: "INT",
            isNullable: false,
            maxLength: 0,
            precision: 0,
            scale: 0,
          },
        ],
        isCTEnabled: true,
        primaryKeyColumnCount: 1,
      },
    },
  ];
  const getTableIDString: spec.ComparisonOptions<string>["getTableIDString"] = (
    s,
  ) => s;
  t.deepEqual(
    spec.compareMetaData({
      previousMetaData,
      latestMetaData,
      getTableIDString,
    }),
    undefined,
    "Metadata must not be considered different.",
  );

  latestMetaData[0].tableMD.columnTypes[0].typeName = "BIGINT";

  t.deepEqual(
    spec.compareMetaData({
      previousMetaData,
      latestMetaData,
      getTableIDString,
    }),
    {
      ...previousMetaData,
      table: {
        mdVersion: 1,
        columns: {
          col: {
            ...previousMetaData["table"].columns["col"],
            typeName: "BIGINT",
          },
        },
      },
    },
    "Metadata must be considered different, and include non-existant table",
  );
});

test("New table detected correctly", (t) => {
  const previousMetaData: spec.ComparisonOptions<string>["previousMetaData"] = {
    table: {
      mdVersion: 0,
      columns: {
        col: {
          typeName: "INT",
          isNullable: false,
          isPrimaryKey: true,
          maxLength: 0,
          precision: 0,
          scale: 0,
        },
      },
    },
    anotherTable: {
      mdVersion: 0,
      columns: {},
    },
  };
  const latestMetaData: spec.ComparisonOptions<string>["latestMetaData"] = [
    {
      tableID: "new-table",
      tableMD: {
        columnNames: ["col"],
        columnTypes: [
          {
            typeName: "INT",
            isNullable: false,
            maxLength: 0,
            precision: 0,
            scale: 0,
          },
        ],
        isCTEnabled: true,
        primaryKeyColumnCount: 1,
      },
    },
    {
      tableID: "table",
      tableMD: {
        columnNames: ["col"],
        columnTypes: [
          {
            typeName: "INT",
            isNullable: false,
            maxLength: 0,
            precision: 0,
            scale: 0,
          },
        ],
        isCTEnabled: true,
        primaryKeyColumnCount: 1,
      },
    },
  ];
  const getTableIDString: spec.ComparisonOptions<string>["getTableIDString"] = (
    s,
  ) => s;
  t.deepEqual(
    spec.compareMetaData({
      previousMetaData,
      latestMetaData,
      getTableIDString,
    }),
    {
      ...previousMetaData,
      "new-table": {
        mdVersion: 0,
        columns: {
          col: previousMetaData["table"].columns["col"],
        },
      },
    },
    "Metadata must be considered different.",
  );
});
