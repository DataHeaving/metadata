import * as t from "io-ts";
import * as validation from "@data-heaving/common-validation";

export type SerializedTableColumnInfo = t.TypeOf<
  typeof serializedTableMetaDataColumn
>;

export type SerializedExportedTablesMetaData = t.TypeOf<
  typeof serializedMetaDataTables
>;

export const serializedTableMetaDataColumn = t.type(
  {
    typeName: validation.nonEmptyString,
    maxLength: t.Integer,
    precision: t.Integer,
    scale: t.Integer,
    isNullable: t.boolean,
    isPrimaryKey: t.boolean,
  },
  "TableMetaDataColumn",
);

export const serializedMetaDataTables = t.record(
  t.string,
  t.type({
    mdVersion: validation.intGeqZero,
    columns: t.record(
      t.string,
      serializedTableMetaDataColumn,
      "TableMetaDataColumns",
    ),
  }),
  "TableMetaData",
);
