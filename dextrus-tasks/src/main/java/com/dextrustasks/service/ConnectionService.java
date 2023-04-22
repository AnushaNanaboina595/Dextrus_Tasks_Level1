package com.dextrustasks.service;

import java.sql.Statement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Service;

import com.dextrustasks.common.CC;
import com.dextrustasks.entity.ConnectionProperties;
import com.dextrustasks.entity.TableDescription;
import com.dextrustasks.entity.TableType;

@Service
public class ConnectionService {

	public Connection getConnection(ConnectionProperties connectionProperties) {
		// TODO Auto-generated method stub
		Connection con = CC.getConnection(connectionProperties);
		return con;
	}

	public List<String> getCatalogsList(ConnectionProperties connectionProperties) {
		// TODO Auto-generated method stub
		List<String> catalogs = null;
		Connection con = CC.getConnection(connectionProperties);
		try {
			ResultSet rs = con.createStatement().executeQuery("select name from sys.databases");
			catalogs = new ArrayList<>();
			while (rs.next()) {
				catalogs.add(rs.getString("name"));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return catalogs;
	}

	public List<String> getSchemas(ConnectionProperties connectionProperties, String catalog) {
		// TODO Auto-generated method stub
		List<String> schemasList = new ArrayList<>();
		Connection con = CC.getConnection(connectionProperties);
		String query = "SELECT name FROM \"" + catalog + "\".sys.schemas";

		try {
			ResultSet rs = con.createStatement().executeQuery(query);
			while (rs.next()) {
				schemasList.add(rs.getString("name"));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return schemasList;
	}

	public List<TableType> getViewsAndTables(ConnectionProperties connectionProperties, String catalog, String schema) {
		// TODO Auto-generated method stub
		List<TableType> viewsAndTables = new ArrayList<>();
		Connection con = CC.getConnection(connectionProperties);
		try {
			PreparedStatement ps = con.prepareStatement("use " + catalog + "; " + CC.GET_TABLES_QUERY);
			ps.setString(1, catalog);
			ps.setString(2, schema);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				TableType tableType = new TableType();
				tableType.setTable_name(rs.getString("TABLE_NAME"));
				tableType.setTable_type(rs.getString("TABLE_TYPE"));
				viewsAndTables.add(tableType);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return viewsAndTables;
	}

	public List<TableDescription> getTableDescription(ConnectionProperties connectionProperties, String catalog,
			String schema, String table) {
		// TODO Auto-generated method stub
		List<TableDescription> tableDescList = new ArrayList<>();
		Connection con = CC.getConnection(connectionProperties);
		try {
			PreparedStatement ps = con.prepareStatement("use " + catalog + " ;" + CC.DESCRIPTION_QUERY);
			table = schema + "." + table;
			ps.setString(1, table);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				TableDescription td = new TableDescription();
				td.setColumnName(rs.getString("COLUMN_NAME"));
				td.setDataType(rs.getString("DATA_TYPE"));
				td.setPrecision(rs.getInt("PRECISION"));
				td.setMaxlength(rs.getInt("MAX_LENGTH"));
				td.setIsNullable(rs.getInt("IS_NULLABLE"));
				td.setPrimaryKey(rs.getInt("PRIMARY_KEY"));
				tableDescList.add(td);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return tableDescList;
	}

	public List<List<Object>> getTableData(ConnectionProperties properties, String query) {
		
		List<List<Object>> rows = new ArrayList<>();
		try {
			Connection con = CC.getConnection(properties);
			Statement statement = con.createStatement();
			ResultSet resultSet =statement.executeQuery(query);
			ResultSetMetaData meta = resultSet.getMetaData();
			int columnCount = meta.getColumnCount();
			while (resultSet.next()) {

				List<Object> row = new ArrayList<>();
				for (int i = 1; i <= columnCount; i++) {

					String columnName = meta.getColumnName(i);
					String columnType = meta.getColumnTypeName(i);
					switch (columnType) {
					case "varchar": {
						row.add(columnName + " : " + resultSet.getString(columnName));
						break;
					}
					case "float": {
						row.add(columnName + " : " + resultSet.getFloat(columnName));
						break;
					}
					case "boolean": {
						row.add(columnName + " : " + resultSet.getBoolean(columnName));
						break;
					}
					case "int": {
						row.add(columnName + " : " + resultSet.getInt(columnName));
						break;
					}
					case "timestamp": {
						row.add(columnName + " : " + resultSet.getTimestamp(columnName));
						break;
					}
					case "decimal": {
						row.add(columnName + " : " + resultSet.getBigDecimal(columnName));
						break;
					}
					case "date": {
						row.add(columnName + " : " + resultSet.getDate(columnName));
						break;
					}
					default:
						row.add("!-!-! " + columnName + " : " + resultSet.getObject(columnName));
						System.out.println("Datatype Not available for Column: " + columnName);
					}
				}
				rows.add(row);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rows;
	
	}

	public List<TableType> getTablesAndViewsByPattern(ConnectionProperties properties, String catalog, String pattern) {
		// TODO Auto-generated method stub
		List<TableType> viewsAndTables = new ArrayList<>();
		try {
			Connection connection = CC.getConnection(properties);
			PreparedStatement statement = connection
					.prepareStatement("use " + catalog + "; " + CC.GET_TABLES_BY_PATTERN_QUERY);
			statement.setString(1, pattern);
			ResultSet resultSet = statement.executeQuery();
			while (resultSet.next()) {
				TableType tableType = new TableType();
				tableType.setTable_name(resultSet.getString("TABLE_NAME"));
				tableType.setTable_type(resultSet.getString("TABLE_TYPE"));
				viewsAndTables.add(tableType);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return viewsAndTables;
		
	}
}
