fuelphp mssql driver <br />
add file to <br />
core/classes/database <br />
add file path to core/bootstrap.php <br />
then set mssql connection as : <br />
return array( <br />
		'default' => array( <br />
				'type' => 'mssql', <br />
				'connection' => array( <br />
						'dsn' => "sqlsrv:Server=127.0.0.1;Database=dbName", <br />
						'hostname' => '', <br />
						'username' => "usrname", <br />
						'password' => "password", <br />
						'database' => '', <br />
						'persistent' => false, <br />
						'compress' => false, <br />
				), <br />
				'identifier' => '', <br />
				'table_prefix' => '', <br />
				'charset' => 'utf8', <br />
				'enable_cache' => true, <br />
				'profiling' => false, <br />
		), <br />
);<br />
