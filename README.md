fuelphp mssql driver 
add file to 
core/classes/database 
add file path to core/bootstrap.php 
then set mssql connection as : 
return array( 
		'default' => array( 
				'type' => 'mssql', 
				'connection' => array( 
						'dsn' => "sqlsrv:Server=127.0.0.1;Database=dbName", 
						'hostname' => '', 
						'username' => "usrname", 
						'password' => "password", 
						'database' => '', 
						'persistent' => false, 
						'compress' => false, 
				), 
				'identifier' => '', 
				'table_prefix' => '', 
				'charset' => 'utf8', 
				'enable_cache' => true, 
				'profiling' => false, 
		), 
);
