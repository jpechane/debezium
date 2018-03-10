/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.antlr.mysql;

import io.debezium.antlr.AntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlLexer;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class MySqlAntlrDdlParser extends AntlrDdlParser<MySqlLexer, MySqlParser> {

    private Tables databaseTables;

    @Override
    protected void parse(MySqlParser parser, Tables databaseTables) {
        this.databaseTables = databaseTables;
        MySqlParser.RootContext root = parser.root();

        ParseTreeWalker.DEFAULT.walk(new MySqlDdlParserListener(), root);
    }

    @Override
    protected MySqlLexer createNewLexerInstance(CharStream charStreams) {
        return new MySqlLexer(charStreams);
    }

    @Override
    protected MySqlParser createNewParserInstance(CommonTokenStream commonTokenStream) {
        return new MySqlParser(commonTokenStream);
    }

    @Override
    protected boolean isGrammarInUpperCase() {
        return true;
    }

    @Override
    protected String replaceOneLineComments(String statement) {
        return statement.replaceAll("--(.*)", "/*$1*/");
    }

    private TableId parseQualifiedTableId(MySqlParser.TableNameContext tableNameContext) {
        String fullTableName = tableNameContext.fullId().getText();
        int dotIndex;
        if ((dotIndex = fullTableName.indexOf(".")) > 0) {
            return resolveTableId(fullTableName.substring(0, dotIndex),
                    fullTableName.substring(dotIndex + 1, fullTableName.length()));
        } else {
            return resolveTableId(currentSchema(), fullTableName);
        }
    }

    private String parseColumnName(MySqlParser.UidContext uidContext) {
        return uidContext.getText();
    }

    /**
     * Parser listener for MySQL alter table queries.
     *
     * @author Roman Kuchár <kucharrom@gmail.com>.
     */
    // TODO: Do we want to split one big listener into a smaller ones?
    // TODO: Can be used by some proxy listener described here: https://github.com/antlr/antlr4/issues/841
    private class MySqlDdlParserListener extends MySqlParserBaseListener {

        private TableEditor tableEditor;
        private ColumnEditor columnEditor;
        private int parsingColumnIndex = 0;
        private List<ColumnEditor> columnEditors;

        /*
         * START - Listening events for alter table statements
         */
        @Override
        public void enterAlterTable(MySqlParser.AlterTableContext ctx) {
            TableId tableId = parseQualifiedTableId(ctx.tableName());
            // TODO: should be table created if it does not exists in memory model?
            tableEditor = databaseTables.editOrCreateTable(tableId);
            super.enterAlterTable(ctx);
        }

        @Override
        public void enterAlterByAddColumn(MySqlParser.AlterByAddColumnContext ctx) {
            String columnName = parseColumnName(ctx.uid().get(0));
            columnEditor = Column.editor().name(columnName);
            // TODO: how can i set a column position and update other existing columns position?
            if (ctx.FIRST() != null) {
                //TODO: this new column should have the first position in table
            } else if (ctx.AFTER() != null) {
                String afterColumn = parseColumnName(ctx.uid().get(1));
                //TODO: this column should have position after the specified column
            }
            super.exitAlterByAddColumn(ctx);
        }

        @Override
        public void exitAlterByAddColumn(MySqlParser.AlterByAddColumnContext ctx) {
            tableEditor.addColumn(columnEditor.create());
            signalAlterTable(tableEditor.tableId(), null, statement(ctx.getParent()));
            super.exitAlterByAddColumn(ctx);
        }


        @Override
        public void enterAlterByAddColumns(MySqlParser.AlterByAddColumnsContext ctx) {
            // multiple columns are added. Initialize a list of column editors for them
            columnEditors = new ArrayList<>(ctx.uid().size());
            for (MySqlParser.UidContext uidContext : ctx.uid()) {
                String columnName = parseColumnName(uidContext);
                columnEditors.add(Column.editor().name(columnName));
            }
            super.enterAlterByAddColumns(ctx);
        }

        @Override
        public void exitAlterByAddColumns(MySqlParser.AlterByAddColumnsContext ctx) {
            columnEditors.forEach(columnEditor -> tableEditor.addColumn(columnEditor.create()));
            signalAlterTable(tableEditor.tableId(), null, statement(ctx.getParent()));
            super.exitAlterByAddColumns(ctx);
        }
        /*
         * END - Listening events for alter table statements
         */

        /*
         * START - Listening events for column definition
         */
        @Override
        public void enterColumnDefinition(MySqlParser.ColumnDefinitionContext ctx) {
            if (columnEditors != null) {
                // column editor list is not null when a multiple columns are parsed in one statement
                columnEditor = columnEditors.get(parsingColumnIndex++);
            }
            resolveColumnDataType(ctx.dataType());
            super.enterColumnDefinition(ctx);
        }

        @Override
        public void enterPrimaryKeyColumnConstraint(MySqlParser.PrimaryKeyColumnConstraintContext ctx) {
            columnEditor.optional(false);
            tableEditor.setPrimaryKeyNames(columnEditor.name());
            super.enterPrimaryKeyColumnConstraint(ctx);
        }

        @Override
        public void enterNullNotnull(MySqlParser.NullNotnullContext ctx) {
            columnEditor.optional(ctx.NOT() == null);
            super.enterNullNotnull(ctx);
        }

        @Override
        public void enterDefaultColumnConstraint(MySqlParser.DefaultColumnConstraintContext ctx) {
            columnEditor.generated(true);
            super.enterDefaultColumnConstraint(ctx);
        }

        @Override
        public void enterAutoIncrementColumnConstraint(MySqlParser.AutoIncrementColumnConstraintContext ctx) {
            columnEditor.autoIncremented(true);
            columnEditor.generated(true);
            super.enterAutoIncrementColumnConstraint(ctx);
        }
        /*
         * END - Listening events for column definition
         */

        /*
         * Last caught event for sql statement
         */
        @Override
        public void exitSqlStatement(MySqlParser.SqlStatementContext ctx) {
            // TODO finish the job and send signal event
            // reset global values for next statement that could be parsed with this instance
            tableEditor = null;
            columnEditor = null;
            columnEditors = null;
            parsingColumnIndex = 0;
            super.exitSqlStatement(ctx);
        }

        private void resolveColumnDataType(MySqlParser.DataTypeContext dataTypeContext) {
            String dataTypeName;
            int jdbcType = Types.NULL;
            if (dataTypeContext instanceof MySqlParser.StringDataTypeContext) {
                // CHAR | VARCHAR | TINYTEXT | TEXT | MEDIUMTEXT | LONGTEXT
                MySqlParser.StringDataTypeContext stringDataTypeContext = (MySqlParser.StringDataTypeContext) dataTypeContext;
                dataTypeName = stringDataTypeContext.typeName.getText();

                if (stringDataTypeContext.lengthOneDimension() != null) {
                    Integer length = Integer.valueOf(stringDataTypeContext.lengthOneDimension().decimalLiteral().getText());
                    columnEditor.length(length);
                }
            } else if (dataTypeContext instanceof MySqlParser.DimensionDataTypeContext) {
                // TINYINT | SMALLINT | MEDIUMINT | INT | INTEGER | BIGINT
                // REAL | DOUBLE | FLOAT
                // DECIMAL | NUMERIC | DEC | FIXED
                // BIT | TIME | TIMESTAMP | DATETIME | BINARY | VARBINARY | YEAR
                MySqlParser.DimensionDataTypeContext dimensionDataTypeContext = (MySqlParser.DimensionDataTypeContext) dataTypeContext;
                dataTypeName = dimensionDataTypeContext.typeName.getText();

                Integer length = null;
                Integer scale = null;
                if (dimensionDataTypeContext.lengthOneDimension() != null) {
                    length = Integer.valueOf(dimensionDataTypeContext.lengthOneDimension().decimalLiteral().getText());
                }

                if (dimensionDataTypeContext.lengthTwoDimension() != null) {
                    List<MySqlParser.DecimalLiteralContext> decimalLiterals = dimensionDataTypeContext.lengthTwoDimension().decimalLiteral();
                    length = Integer.valueOf(decimalLiterals.get(0).getText());
                    scale = Integer.valueOf(decimalLiterals.get(1).getText());
                }

                if (dimensionDataTypeContext.lengthTwoOptionalDimension() != null) {
                    List<MySqlParser.DecimalLiteralContext> decimalLiterals = dimensionDataTypeContext.lengthTwoOptionalDimension().decimalLiteral();
                    length = Integer.valueOf(decimalLiterals.get(0).getText());

                    if (decimalLiterals.size() > 1) {
                        scale = Integer.valueOf(decimalLiterals.get(1).getText());
                    }
                }
                if (length != null) {
                    columnEditor.length(length);
                }
                if (scale != null) {
                    columnEditor.scale(scale);
                }
                // TODO: resolve jdbc type
            } else if (dataTypeContext instanceof MySqlParser.SimpleDataTypeContext) {
                // DATE | TINYBLOB | BLOB | MEDIUMBLOB | LONGBLOB | BOOL | BOOLEAN
                dataTypeName = ((MySqlParser.SimpleDataTypeContext) dataTypeContext).typeName.getText();
                // TODO: resolve jdbc type
            } else if (dataTypeContext instanceof MySqlParser.CollectionDataTypeContext) {
                // ENUM | SET
                // do not care about charsetName or collationName
                dataTypeName = ((MySqlParser.CollectionDataTypeContext) dataTypeContext).typeName.getText();
                // TODO: resolve jdbc type
            } else if (dataTypeContext instanceof MySqlParser.SpatialDataTypeContext) {
                // GEOMETRYCOLLECTION | LINESTRING | MULTILINESTRING | MULTIPOINT | MULTIPOLYGON | POINT | POLYGON
                dataTypeName = ((MySqlParser.SpatialDataTypeContext) dataTypeContext).typeName.getText();
                // TODO: resolve jdbc type
            } else {
                throw new IllegalStateException("Not recognized instance of data type context for " + dataTypeContext.getText());
            }

            columnEditor.type(dataTypeName);
            columnEditor.jdbcType(jdbcType);
        }

    }

}
