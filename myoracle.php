<?php

error_reporting(E_ALL);
ini_set('display_errors', 1);
date_default_timezone_set('Asia/Manila');

$arguments = getopt("", array("old_table:", "new_table:", "primary_key:"));
$hint = "Execute command like this:\n\n";
$hint .= "php myoracle.php";
$hint .= " --old_table=\"<mysql_table_name>\"";
$hint .= " --new_table=\"<oracle_table_name>\"";
$hint .= " --primary_key=\"<mysql_table_primary_key>\"\n\n";

if(file_exists('./database.php')){
  require_once('./database.php');
} else {
  die('Configure database first.');
}

$error = "";

if(isset($arguments['old_table'])) {
  if(empty($arguments['old_table'])) {
      $error .= "--old_table not specified.\n";
  } else {
    $old_table_name = $arguments['old_table'];
  }
} else {
    $valid = 0;
    $error .= "--old_table not specified.\n";
}

if(isset($arguments['new_table'])) {
  if(empty($arguments['new_table'])) {
      $error .= "--new_table not specified.\n";
  } else {
    $new_table_name = $arguments['new_table'];
  }
} else {
    $error .= "--new_table not specified.\n";
}

if(isset($arguments['primary_key'])) {
  if(empty($arguments['primary_key'])) {
      $valid = 0;
      $error .= "--primary_key not specified.\n";
  } else {
    $primary_key = $arguments['primary_key'];
  }
} else {
    $valid = 0;
    $error .= "--primary_key not specified.\n";
}

if(!empty($error)){
  die("\nErrors found:\n".$error."\n".$hint);
}

$table_query = "";
$column_type_array = Array();
$default_array = Array();
$column_name_array = Array();
$select_columns = "";
$procedure_params = "";
$procedure_param_type = "";
$procedure_vars = "";

// Create connection
$conn = new mysqli(DB_HOSTNAME, DB_USERNAME, DB_PASSWORD, DB_DATABASE);
// Check connection
if ($conn->connect_error) {
  die("Connection failed: " . $conn->connect_error);
} else {
  echo "Successfully connected to ".DB_DATABASE." @ ".DB_HOSTNAME."\n";
}

# create directory
if(!file_exists($old_table_name)){
  mkdir($old_table_name);
}

// check type
function typeChecker($haystack, $needles) {
  if(!is_array($needles)) $needles = array($needles);
  foreach($needles as $what) {
      if(($pos = strpos(strtolower($haystack), $what))!==false) return true;
  }
  return false;
}

// get length 
function getLength($string){
  preg_match('#\((.*?)\)#', $string, $match);
  return $match[1];
}

// build table
$table_query = "CREATE TABLE {$new_table_name}_TBL (\n";

$sql = "SHOW COLUMNS FROM ".$old_table_name;
$columns = $conn->query($sql);

foreach($columns->fetch_all(MYSQLI_ASSOC) as $index => $row){
  $separator = ($index + 1) == mysqli_num_rows($columns) ? "" : ",";
  $column_name = $row['Field'];
  $type = strtolower($row['Type']);
  $null = $row['Null'];
  $key = $row['Key'];
  $field = $row['Field'];
  $default = isset($row['Default']) ? "'{$row['Default']}'" : "NULL";
  $extra = $row['Extra'];

  // analyze column datatype
	if (typeChecker($type, ['int', 'tinyint'])) {
    $type = "INTEGER";
    $procedure_param_type = "INTEGER";
  } else if (typeChecker($type, ['decimal'])) {
    $length = getLength($type);
    $type = "NUMBER({$length})";
    $procedure_param_type = "NUMBER";
  } else if (typeChecker($type, ['varchar'])) {
    $length = getLength($type);
    $type = "VARCHAR2({$length})";
    $procedure_param_type = "VARCHAR2";
  } else if (typeChecker($type, ['text'])) {
    $type = "CLOB";
    $procedure_param_type = "CLOB";
  } else if (typeChecker($type, ['datetime'])) {
    $type = "TIMESTAMP";
    $procedure_param_type = "TIMESTAMP";
    $default = "NULL";
  } else if (typeChecker($type, ['date'])) {
    $type = "DATE";
    $procedure_param_type = "DATE";
    $default = "NULL";
  } else {
    $type = "ERROR";
    $procedure_param_type = "ERROR";
  }
  
  // add column type
  array_push($column_type_array, $type);
  array_push($default_array, $default);
  array_push($column_name_array, $column_name);

  $table_query .= " {$column_name} {$type} DEFAULT {$default}{$separator}\n";
  $procedure_params .= " {$column_name} IN {$procedure_param_type}{$separator}\n";
  $procedure_vars .= " {$column_name}{$separator}\n";
  $select_columns .= "{$column_name}{$separator} ";
}

$table_query .= ");\n\n";

$table_query .= "ALTER TABLE {$new_table_name}_TBL ADD (
                  CONSTRAINT {$primary_key}x PRIMARY KEY ({$primary_key}));";

// generate file for table query
file_put_contents("./{$old_table_name}/{$new_table_name}_TBL_structure.sql", $table_query);
echo "\nDone Generating: {$new_table_name}_TBL_structure.sql";

// build mysql select statement to use in data import for SQLDeveloper
$keys = explode(",", $procedure_vars);

$mysql_select = "SELECT\n";
foreach($keys as $index => $column){
  $column = trim($column);
  $separator = count($keys) == ($index + 1) ? "" : ",";
  
  if($column_type_array[$index] == 'TIMESTAMP'){
    // format datetime for Oracle timestamp
    $column = "(CASE WHEN DATE_FORMAT({$column}, '%Y/%m/%d %H:%i:%s') = '0000/00/00 00:00:00' THEN NULL ELSE DATE_FORMAT({$column}, '%Y/%m/%d %H:%i:%s') END) {$column}";
    // $column = "CASE WHEN DATE_FORMAT({$column}, '%m/%d/%Y %r') = '00/00/0000 12:00:00 AM' THEN NULL ELSE DATE_FORMAT({$column}, '%m/%d/%Y %r') END {$column}";
  } else if($column_type_array[$index] == 'DATE'){
    // format date for Oracle date
    $column = "(CASE WHEN DATE_FORMAT({$column}, '%m/%d/%Y') = '00/00/0000' THEN NULL ELSE DATE_FORMAT({$column}, '%m/%d/%Y') END) {$column}";
  } else {
    // replace defaults to NULL
    if($default_array[$index] != 'NULL'){
      $column = "(COALESCE({$column}, {$default_array[$index]})) {$column}";
    }
  }
  $mysql_select .= "{$column}{$separator}\n";
}
$mysql_select .= "\tFROM {$old_table_name};";

// generate file for table query
file_put_contents("./{$old_table_name}/{$old_table_name}_select.sql", $mysql_select);
echo "\nDone Generating: {$old_table_name}_select.sql";

// generate csv file for data
$data = $conn->query($mysql_select);
$rows = $data->fetch_all(MYSQLI_ASSOC);

$data_csv = fopen("./{$old_table_name}/{$old_table_name}.csv","w");

$header_csv = "";
fputcsv($data_csv, $column_name_array);
foreach ($rows as $row) {
  $output = Array();
  foreach($row as $value){
    array_push($output, str_replace(array("\r","\n"),"", trim(preg_replace('/\s+/', ' ', $value))));
  }
  fputcsv($data_csv, $output);
}

fclose($data_csv);
echo "\nDone Generating: {$old_table_name}.csv";

// build insert stored procedure
$stored_procedure_query = "CREATE OR REPLACE PROCEDURE {$new_table_name}_INSERT_PRC (
                            \t{$procedure_params}
                          )
                          AS
                          BEGIN
                            \tINSERT INTO {$new_table_name}_TBL({$select_columns}) VALUES({$procedure_vars});
                            \tCOMMIT;
                          END;";
file_put_contents("./{$old_table_name}/{$new_table_name}_INSERT_procedure.sql", $stored_procedure_query);
echo "\nDone Generating: {$new_table_name}_INSERT_procedure.sql";

// build update stored procedure
$stored_procedure_query = "CREATE OR REPLACE PROCEDURE {$new_table_name}_UPDATE_PRC (
                            \t{$procedure_params}
                          )
                          AS
                          BEGIN
                            \tUPDATE {$new_table_name}_TBL
                            \tSET \n";
foreach($keys as $index => $value){
  $value = trim($value);
  if($index > 0){
    $separator = count($keys) == $index || $index == 1 ? "" : ",";
    $stored_procedure_query .= "{$separator}\t{$value} = {$value}\n";
  }
}
$stored_procedure_query .= " \tWHERE {$primary_key} = {$primary_key};
                            \tCOMMIT;
                          END;";
file_put_contents("./{$old_table_name}/{$new_table_name}_UPDATE_procedure.sql", $stored_procedure_query);
echo "\nDone Generating: {$new_table_name}_UPDATE_procedure.sql";

// build delete stored procedure
$stored_procedure_query = "CREATE OR REPLACE PROCEDURE {$new_table_name}_DELETE_PRC (
                            \t{$primary_key} INTEGER
                          )
                          AS
                          BEGIN
                            \tDELETE FROM {$new_table_name}_TBL WHERE {$primary_key} = {$primary_key};
                            \tCOMMIT;
                          END;";
file_put_contents("./{$old_table_name}/{$new_table_name}_DELETE_procedure.sql", $stored_procedure_query);
echo "\nDone Generating: {$new_table_name}_DELETE_procedure.sql";

// build data query 
$select_sql = "SELECT {$select_columns} FROM {$old_table_name}";
$data = $conn->query($select_sql);

$insert_query = "";
$values = "";
$index = 0;
$last_primary_key_id = 0;

foreach($data->fetch_all(MYSQLI_ASSOC) as $row){
  $values = "";
  foreach($row as $key => $column_value){
    if($key == $primary_key){
      $last_primary_key_id = $column_value + 1; 
    }
    $index += 1;
    $separator = count($column_type_array) == $index ? "" : ",";
    $column_value = empty($column_value) ? "NULL" : "'".str_replace("'", "''", trim($column_value))."'";
    // CHECK IF DATE OR DATETIME
    if(typeChecker($column_type_array[($index-1)], ['timestamp']) && $column_value != "NULL"){
      $column_value = str_replace("'", "", $column_value);
      if($column_value == "0000-00-00 00:00:00"){
        $column_value = "NULL";
      } else {
        $column_value = "TO_TIMESTAMP('".date("d-M-Y H:i:s", strtotime($column_value))."', 'DD-Mon-RR HH24:MI:SS.FF')";
      }
    }
    if(typeChecker($column_type_array[($index-1)], ['date']) && $column_value != "NULL"){
      $column_value = str_replace("'", "", $column_value);
      if($column_value == "0000-00-00"){
        $column_value = "NULL";
      } else {
        $column_value = "'".date("m/d/Y", strtotime($column_value))."'";
      }
    }
    $values .= "{$column_value}{$separator}";
  }
  $insert_query .= "EXECUTE {$new_table_name}_INSERT_PRC({$values});\n";
  $index = 0;
}

// output data file
try {
  // REMOVED
  // file_put_contents("./{$old_table_name}/{$new_table_name}_TBL_data.sql", "SET DEFINE OFF;\n\n".$insert_query) or "Error generating too many data.";
  // echo "\nDone Generating: {$new_table_name}_TBL_data.sql";
} catch(Exception $e) {
  echo 'Error: ' .$e->getMessage();
}


/*
# Multi Insert Query
$select_sql = "SELECT {$select_columns} FROM {$old_table_name}";
$data = $conn->query($select_sql);

$insert_query = "";
$into_query = " INTO {$old_table_name} ({$select_columns})";
$values = "";
$index = 0;
$last_primary_key_id = 0;

foreach($data->fetch_all(MYSQLI_ASSOC) as $row){
  $values = "";
  foreach($row as $key => $column_value){
    if($key == $primary_key){
      $last_primary_key_id = $column_value + 1; 
    }
    $index += 1;
    $separator = count($column_type_array) == $index ? "" : ",";
    $column_value = empty($column_value) ? "NULL" : "'".str_replace("'", "''", $column_value)."'";
    // CHECK IF DATE OR DATETIME
    if(typeChecker($column_type_array[($index-1)], ['timestamp']) && $column_value != "NULL"){
      $column_value = str_replace("'", "", $column_value);
      $column_value = "TO_TIMESTAMP('".date("d/M/Y H:i:s", strtotime($column_value))."', 'DD-Mon-RR HH24:MI:SS.FF')";
    }
    if(typeChecker($column_type_array[($index-1)], ['date']) && $column_value != "NULL"){
      $column_value = str_replace("'", "", $column_value);
      $column_value = "'".date("m/d/Y", strtotime($column_value))."'";
    }
    $values .= "{$column_value}{$separator}";
  }
  $insert_query .= "\tINTO {$new_table_name}_TBL({$select_columns}) VALUES ({$values}) \n";
  $index = 0;
}
$insert_query = "SET DEFINE OFF;\n\nINSERT ALL \n{$insert_query}SELECT * FROM dual;";
// output data file
file_put_contents("./{$old_table_name}/{$new_table_name}_TBL_data.sql", $insert_query);
echo "\nDone Generating: {$new_table_name}_TBL_data.sql";
*/


// generate ID using sequence and trigger
$sequence = "CREATE SEQUENCE {$new_table_name}_SEQ START WITH {$last_primary_key_id} INCREMENT BY 1;\n\n";

$sequence .= "CREATE OR REPLACE TRIGGER {$new_table_name}_INSERT_TRG\n";
$sequence .= " BEFORE INSERT ON {$new_table_name}_TBL FOR EACH ROW\n";
$sequence .= " WHEN (NEW.{$primary_key} IS NULL)\n";
$sequence .= "BEGIN\n";
$sequence .= " SELECT {$new_table_name}_SEQ.NEXTVAL INTO :NEW.{$primary_key} FROM DUAL;\n";
$sequence .= "END;\n";

// output sequence and trigger file
file_put_contents("./{$old_table_name}/{$new_table_name}_SEQ_AND_TRG.sql", $sequence);
echo "\nDone Generating: {$new_table_name}_SEQ_AND_TRG.sql";

?>