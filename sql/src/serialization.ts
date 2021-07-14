import { isDeepStrictEqual } from "util";
import * as common from "@data-heaving/common";
import * as sql from "@data-heaving/common-sql";
import * as common_validation from "@data-heaving/common-validation";
import * as types from "./types";

export const readExistingMetaDataAndWriteIfDifferent = async <TTableID>(
  {
    latestMetaData,
    getTableIDString,
  }: Pick<ComparisonOptions<TTableID>, "latestMetaData" | "getTableIDString">,
  {
    readExistingData,
    writeNewData,
  }: common.ObjectStorageFunctionality<types.SerializedExportedTablesMetaData>,
) => {
  const newMD = compareMetaData({
    previousMetaData: await common_validation.retrieveValidatedDataFromStorage(
      readExistingData,
      types.serializedMetaDataTables.decode,
    ),
    latestMetaData,
    getTableIDString,
  });

  if (newMD) {
    await writeNewData(newMD);
  }
  return newMD;
};

export interface ComparisonOptions<TTableID> {
  previousMetaData: types.SerializedExportedTablesMetaData | undefined;
  latestMetaData:
    | types.SerializedExportedTablesMetaData
    | ReadonlyArray<{
        tableID: TTableID;
        tableMD: sql.TableMetaData;
      }>;
  getTableIDString: (tableID: TTableID) => string;
}

export const compareMetaData = <TTableID>({
  previousMetaData,
  latestMetaData,
  getTableIDString,
}: ComparisonOptions<TTableID>) => {
  let tablesMDWasDifferent = false;
  let metadata: types.SerializedExportedTablesMetaData;
  if (Array.isArray(latestMetaData)) {
    metadata = latestMetaData.reduce<types.SerializedExportedTablesMetaData>(
      (tableDictionary, { tableID: tableIDObj, tableMD }) => {
        const tableID = getTableIDString(tableIDObj);
        const existingTableMDColumns = previousMetaData?.[tableID]?.columns;
        const thisTableColumns = tableMD.columnNames.reduce<
          types.SerializedExportedTablesMetaData[string]["columns"]
        >((columns, columnName, columnIndex) => {
          columns[columnName] = {
            ...tableMD.columnTypes[columnIndex],
            isPrimaryKey: columnIndex < tableMD.primaryKeyColumnCount,
          };
          return columns;
        }, {});
        const existingMDVersion = previousMetaData?.[tableID]?.mdVersion;
        let mdVersion = existingMDVersion || 0;
        if (
          existingTableMDColumns &&
          !isDeepStrictEqual(thisTableColumns, existingTableMDColumns)
        ) {
          // We have noticed a change - increment version
          ++mdVersion;
        }
        if (!tablesMDWasDifferent) {
          tablesMDWasDifferent = mdVersion !== existingMDVersion;
        }
        tableDictionary[tableID] = {
          mdVersion,
          columns: thisTableColumns,
        };
        return tableDictionary;
      },
      {},
    );
  } else {
    metadata = latestMetaData;
    tablesMDWasDifferent = !isDeepStrictEqual(previousMetaData, latestMetaData);
  }

  // Notice that we might get table deletion in SQL, but we probably don't want to remove it from MD.
  if (previousMetaData) {
    for (const [tableID, tableMD] of Object.entries(previousMetaData)) {
      if (!(tableID in metadata)) {
        // There existed table MD which is now gone, but let's not wipe it
        metadata[tableID] = tableMD;
        if (!tablesMDWasDifferent) {
          tablesMDWasDifferent = true;
        }
      }
    }
  }

  return tablesMDWasDifferent ? metadata : undefined;
};

// temp-fix for this: https://github.com/microsoft/TypeScript/issues/17002
declare global {
  interface ArrayConstructor {
    isArray(arg: ReadonlyArray<any> | any): arg is ReadonlyArray<any>; // eslint-disable-line
  }
}
