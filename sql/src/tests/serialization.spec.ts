import test from "ava";
import * as common from "@data-heaving/common";
import * as spec from "../serialization";
import * as types from "../types";

test("Previously existing table affects result when latest is array without that table", (t) => {
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
  const getTableIDString = (s: string) => s;
  t.deepEqual(
    spec.compareMetaData({
      previousMetaData,
      latestMetaData,
      getTableIDString,
    }),
    previousMetaData,
    "Metadata must be considered different.",
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

test("Test that readExistingMetaDataAndWriteIfDifferent works as expected", async (t) => {
  let storedMD: types.SerializedExportedTablesMetaData | undefined = undefined;
  const storage: common.ObjectStorageFunctionality<types.SerializedExportedTablesMetaData> = {
    storageID: "In-memory storage for testing purposes",
    readExistingData: () => Promise.resolve(storedMD),
    writeNewData: (md) => {
      storedMD = md;
      return Promise.resolve();
    },
  };
  const latestMetaData = [
    {
      tableID: "table",
      tableMD: {
        columnNames: ["col"],
        columnTypes: [
          {
            typeName: "VARCHAR",
            isNullable: false,
            maxLength: 0,
            precision: 0,
            scale: 0,
          },
        ],
        isCTEnabled: false,
        primaryKeyColumnCount: 1,
      },
    },
  ] as const;
  const getTableIDString = (t: string) => t;

  let newMD = await spec.readExistingMetaDataAndWriteIfDifferent(
    {
      latestMetaData,
      getTableIDString,
    },
    storage,
  );
  const expectedMD: types.SerializedExportedTablesMetaData = {
    table: {
      mdVersion: 0,
      columns: {
        col: {
          typeName: "VARCHAR",
          isNullable: false,
          isPrimaryKey: true,
          maxLength: 0,
          precision: 0,
          scale: 0,
        },
      },
    },
  };
  t.deepEqual(newMD, expectedMD, "Initial write must be seen as changeful.");
  t.is(
    storedMD as unknown,
    newMD as unknown,
    "Correct value must have been passed to storage",
  ); // There is some TS bug which causes storedMD to be of type 'undefined' at this point...

  newMD = await spec.readExistingMetaDataAndWriteIfDifferent(
    {
      latestMetaData,
      getTableIDString,
    },
    storage,
  );
  t.deepEqual(
    newMD,
    undefined,
    "Subsequent write must not be seen as changeful",
  );
  t.deepEqual(
    storedMD as unknown,
    expectedMD as unknown,
    "Stored value must have been unchanged",
  );

  newMD = await spec.readExistingMetaDataAndWriteIfDifferent(
    {
      latestMetaData: [
        {
          tableID: "table",
          tableMD: {
            columnNames: ["col", "col2"],
            columnTypes: [
              {
                typeName: "VARCHAR",
                isNullable: false,
                maxLength: 0,
                precision: 0,
                scale: 0,
              },
              {
                typeName: "INT",
                isNullable: true,
                maxLength: 0,
                precision: 0,
                scale: 0,
              },
            ],
            isCTEnabled: false,
            primaryKeyColumnCount: 1,
          },
        },
      ] as const,
      getTableIDString: (t) => t,
    },
    storage,
  );
  const newExpectedMD: types.SerializedExportedTablesMetaData = {
    table: {
      mdVersion: 1, // Notice the increase in MD version!
      columns: {
        col: {
          typeName: "VARCHAR",
          isNullable: false,
          isPrimaryKey: true,
          maxLength: 0,
          precision: 0,
          scale: 0,
        },
        col2: {
          typeName: "INT",
          isNullable: true,
          isPrimaryKey: false,
          maxLength: 0,
          precision: 0,
          scale: 0,
        },
      },
    },
  };
  t.deepEqual(newMD, newExpectedMD, "Final write must be seen as changeful");
  t.deepEqual(
    storedMD as unknown,
    newExpectedMD as unknown,
    "Final write must have written new MD to storage",
  );
});

test("The compareMetaData works also when latestMetaData is record and not just array", (t) => {
  const getTableIDString = () => {
    throw new Error("This should not be used when latestMetaData is record");
  };
  let comparisonResult = spec.compareMetaData({
    previousMetaData: {},
    latestMetaData: {},
    getTableIDString,
  });
  t.deepEqual(comparisonResult, undefined);

  const latestMetaData: types.SerializedExportedTablesMetaData = {
    table: {
      mdVersion: 0,
      columns: {},
    },
  };
  comparisonResult = spec.compareMetaData({
    previousMetaData: {},
    latestMetaData,
    getTableIDString,
  });
  t.deepEqual(comparisonResult, latestMetaData);
});

test("The compareMetaData behaves as expected when table is deleted from metadata", (t) => {
  const mdWithTwoTables: types.SerializedExportedTablesMetaData = {
    table: {
      mdVersion: 0,
      columns: {},
    },
    table2: {
      mdVersion: 0,
      columns: {},
    },
  };
  const mdWithOneTable: types.SerializedExportedTablesMetaData = {
    table: mdWithTwoTables["table"],
  };
  const getTableIDString = () => {
    throw new Error("This should not be used when latestMetaData is record");
  };
  let comparisonResult = spec.compareMetaData({
    previousMetaData: mdWithTwoTables,
    latestMetaData: mdWithOneTable,
    getTableIDString,
  });
  t.deepEqual(
    comparisonResult,
    mdWithTwoTables,
    "Comparison result must include newly deleted table.",
  );

  comparisonResult = spec.compareMetaData({
    previousMetaData: mdWithTwoTables,
    latestMetaData: [
      {
        tableID: "table",
        tableMD: {
          columnNames: [],
          columnTypes: [],
          isCTEnabled: false,
          primaryKeyColumnCount: 0,
        },
      },
    ],
    getTableIDString: (name) => name,
  });
  t.deepEqual(
    comparisonResult,
    mdWithTwoTables,
    "Comparison result must include newly deleted table also when using array.",
  );
});
