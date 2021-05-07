import { isDeepStrictEqual } from "util";
import * as common from "@data-heaving/common";
import * as common_validation from "@data-heaving/common-validation";
import * as types from "./types";

export type MetaDataFunctionality = common.ObjectStorageFunctionality<types.SerializedExportedTablesMetaData>;
export const performMDWrite = async <TTableID>(
  tables: ReadonlyArray<{
    tableID: TTableID;
    tableMD: common.TableMetaData;
  }>,
  getTableIDString: (tableID: TTableID) => string,
  { readExistingData, writeNewDataWhenDifferent }: MetaDataFunctionality,
) => {
  const existingMD = await common_validation.retrieveValidatedDataFromStorage(
    readExistingData,
    types.serializedMetaDataTables.decode,
  );
  let tablesMDWasDifferent = false;
  const metadata = tables.reduce<types.SerializedExportedTablesMetaData>(
    (tableDictionary, { tableID: tableIDObj, tableMD }) => {
      const tableID = getTableIDString(tableIDObj);
      const existingTableMDColumns = existingMD?.[tableID]?.columns;
      const thisTableColumns = tableMD.columnNames.reduce<
        types.SerializedExportedTablesMetaData[string]["columns"]
      >((columns, columnName, columnIndex) => {
        columns[columnName] = {
          ...tableMD.columnTypes[columnIndex],
          isPrimaryKey: columnIndex < tableMD.primaryKeyColumnCount,
        };
        return columns;
      }, {});
      const existingMDVersion = existingMD?.[tableID]?.mdVersion;
      let mdVersion = existingMDVersion || 0;
      if (
        existingTableMDColumns &&
        !isDeepStrictEqual(thisTableColumns, existingTableMDColumns)
      ) {
        // We have noticed a change - increment version
        ++mdVersion;
      }
      tablesMDWasDifferent = mdVersion !== existingMDVersion;
      tableDictionary[tableID] = {
        mdVersion,
        columns: thisTableColumns,
      };
      return tableDictionary;
    },
    {},
  );
  // Notice that we might get table deletion in SQL, but we probably don't want to remove it from MD.
  if (existingMD) {
    for (const [tableID, tableMD] of Object.entries(existingMD)) {
      if (!(tableID in metadata)) {
        // There existed table MD which is now gone, but let's not wipe it
        metadata[tableID] = tableMD;
        tablesMDWasDifferent = true;
      }
    }
  }
  if (tablesMDWasDifferent) {
    await writeNewDataWhenDifferent(metadata);
  }
  return {
    tablesMDWasDifferent,
    metadata,
  };
};

export * from "./types";
