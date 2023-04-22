package com.dextrustasks.controller;

import java.sql.Connection;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.dextrustasks.entity.ConnectionProperties;
import com.dextrustasks.entity.RequestBodyPattern;
import com.dextrustasks.entity.RequestBodyQuery;
import com.dextrustasks.entity.TableDescription;
import com.dextrustasks.entity.TableType;
import com.dextrustasks.service.ConnectionService;

@RestController
@RequestMapping("/dextrus")
public class ConnectionController {
	@Autowired
	ConnectionService connectionService;
  @PostMapping("/connect")
  public ResponseEntity<?> getConnection(@RequestBody ConnectionProperties connectionProperties) {
	 Connection con=connectionService.getConnection(connectionProperties);
	 if(con!=null)
		 return new ResponseEntity<>("connected",HttpStatus.OK);
	 else
	return new ResponseEntity<>("not connected",HttpStatus.SERVICE_UNAVAILABLE);
	  
  }
  @PostMapping("/catalogs")
  public ResponseEntity<List<String>> getCatalogs(@RequestBody ConnectionProperties connectionProperties){
	  List<String> catalogs=connectionService.getCatalogsList(connectionProperties);
	return new ResponseEntity<List<String>>(catalogs,HttpStatus.OK);
	  }
  
  @PostMapping("/catalogs/{catalog}/schemas")
  public ResponseEntity<List<String>> getSchemas(@RequestBody ConnectionProperties connectionProperties,@PathVariable String catalog){
	  List<String> schemas = connectionService.getSchemas(connectionProperties,catalog);
	return new ResponseEntity<List<String>>(schemas,HttpStatus.OK);
	  
  }
  
  @PostMapping("/catalogs/{catalog}/schemas/{schema}/viewsandtables")
  public ResponseEntity<List<?>> getTablesAndViews(@RequestBody ConnectionProperties connectionProperties,@PathVariable String catalog,@PathVariable String schema){
	List<TableType> viewsAndTables=connectionService.getViewsAndTables(connectionProperties,catalog,schema);
	  return new ResponseEntity<>(viewsAndTables,HttpStatus.OK);
	  
  }
  
  @PostMapping("/catalogs/{catalog}/schemas/{schema}/viewsandtables/{table}")
  public ResponseEntity<List<TableDescription>> getColumnProperties(@RequestBody ConnectionProperties connectionProperties,@PathVariable String catalog,@PathVariable String schema,@PathVariable String table){
	  List<TableDescription> tableDescList=connectionService.getTableDescription(connectionProperties,catalog,schema,table);
	return  new ResponseEntity<List<TableDescription>>(tableDescList,HttpStatus.OK);
	  
  }
  @PostMapping("/query")
	public ResponseEntity<List<List<Object>>> test(@RequestBody RequestBodyQuery queryBody ) {
		ConnectionProperties prop = queryBody.getProperties();
		String query = queryBody.getQuery();
		List<List<Object>> tableDataList =connectionService.getTableData(prop, query);
		return new ResponseEntity<List<List<Object>>>(tableDataList, HttpStatus.OK);
	}
  @PostMapping("/search")
	public ResponseEntity<List<TableType>> getTablesByPattern(@RequestBody RequestBodyPattern bodyPattern){
		List<TableType> viewsAndTables = connectionService.getTablesAndViewsByPattern(bodyPattern.getProperties(),bodyPattern.getCatalog(),bodyPattern.getPattern());
		return new ResponseEntity<List<TableType>>(viewsAndTables, HttpStatus.OK);
	}
	
}
