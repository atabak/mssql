<?php

namespace Fuel\Core;

class Database_Mssql_Connection extends \Database_PDO_Connection
{

    protected $_connection;
    protected $_identifier = '';

    protected function __construct($name, array $config)
    {
        // construct a custom schema driver
//		$this->_schema = new \Database_Drivername_Schema($name, $this);
        // call the parent consructor
        // this driver only works on Windows
        if (strpos(php_uname('s'), 'Windows') === false)
        {
            throw new \Database_Exception('The "SQLSRV" database driver works only on Windows. On *nix, use the "DBLib" driver instead.');
        }

        parent::__construct($name, $config);

        if (isset($config['identifier']))
        {
            // Allow the identifier to be overloaded per-connection
            $this->_identifier = (string) $this->_config['identifier'];
        }
    }

    public function connect()
    {
        if ($this->_connection)
        {
            return;
        }

        // make sure we have all connection parameters
        $this->_config = array_merge(array(
            'connection'   => array(
                'dsn'        => '',
                'hostname'   => '',
                'username'   => null,
                'password'   => null,
                'database'   => '',
                'persistent' => false,
                'compress'   => false,
            ),
            'identifier'   => '`',
            'table_prefix' => '',
            'charset'      => 'utf8',
            'collation'    => false,
            'enable_cache' => true,
            'profiling'    => false,
            'readonly'     => false,
            'attrs'        => array(),
                ), $this->_config);

        // Force PDO to use exceptions for all errors
        // remove this
        /*
          $this->_config['attrs'] = array(
          \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
          );
         */
        if (!empty($this->_config['connection']['persistent']))
        {
            // Make the connection persistent
            $this->_config['attrs'][\PDO::ATTR_PERSISTENT] = true;
        }

        try
        {
            // Create a new PDO connection
            $this->_connect();
        }
        catch (\PDOException $e)
        {
            // and convert the exception in a database exception
            if (!is_numeric($error_code = $e->getCode()))
            {
                if ($this->_connection)
                {
                    $error_code = $this->_connection->errorinfo();
                    $error_code = $error_code[1];
                }
                else
                {
                    $error_code = 0;
                }
            }
            throw new \Database_Exception(str_replace($this->_config['connection']['password'], str_repeat('*', 10), $e->getMessage()), $error_code, $e);
        }
    }

    public function disconnect()
    {
        // destroy the PDO object
        $this->_connection = null;

        // and reset the savepoint depth
        $this->_transaction_depth = 0;

        return true;
    }

    public function driver_name()
    {
        // Make sure the database is connected
        $this->_connection or $this->connect();

        // Getting driver name
        return $this->_connection->getAttribute(\PDO::ATTR_DRIVER_NAME);
    }

    public function set_charset($charset)
    {
        if ($charset == 'utf8' or $charset = 'utf-8')
        {
            // use utf8 encoding
            $this->_connection->setAttribute(\PDO::SQLSRV_ATTR_ENCODING, \PDO::SQLSRV_ENCODING_UTF8);
        }
        elseif ($charset == 'system')
        {
            // use system encoding
            $this->_connection->setAttribute(\PDO::SQLSRV_ATTR_ENCODING, \PDO::SQLSRV_ENCODING_SYSTEM);
        }
        elseif (is_numeric($charset))
        {
            // charset code passed directly
            $this->_connection->setAttribute(\PDO::SQLSRV_ATTR_ENCODING, $charset);
        }
        else
        {
            // unknown charset, use the default encoding
            $this->_connection->setAttribute(\PDO::SQLSRV_ATTR_ENCODING, \PDO::SQLSRV_ENCODING_DEFAULT);
        }
    }

    public function query($type, $sql, $as_object)
    {
        // Make sure the database is connected
        $this->_connection or $this->connect();

        if (!empty($this->_config['profiling']))
        {
            // Get the paths defined in config
            $paths = \Config::get('profiling_paths');

            // Storage for the trace information
            $stacktrace = array();

            // Get the execution trace of this query
            $include = false;
            foreach (debug_backtrace() as $index => $page)
            {
                // Skip first entry and entries without a filename
                if ($index > 0 and empty($page['file']) === false)
                {
                    // Checks to see what paths you want backtrace
                    foreach ($paths as $index => $path)
                    {
                        if (strpos($page['file'], $path) !== false)
                        {
                            $include = true;
                            break;
                        }
                    }

                    // Only log if no paths we defined, or we have a path match
                    if ($include or empty($paths))
                    {
                        $stacktrace[] = array('file' => \Fuel::clean_path($page['file']), 'line' => $page['line']);
                    }
                }
            }

            $benchmark = \Profiler::start($this->_instance, $sql, $stacktrace);
        }

        // run the query. if the connection is lost, try 3 times to reconnect
        $attempts = 3;

        do
        {
            try
            {
                // try to run the query
                $result = $this->_connection->query($sql);
                break;
            }
            catch (\Exception $e)
            {
                // if failed and we have attempts left
                if ($attempts > 0)
                {
                    // try reconnecting if it was a MySQL disconnected error
                    if (strpos($e->getMessage(), '2006 MySQL') !== false)
                    {
                        $this->disconnect();
                        $this->connect();
                    }
                    else
                    {
                        // other database error, cleanup the profiler
                        isset($benchmark) and \Profiler::delete($benchmark);

                        // and convert the exception in a database exception
                        if (!is_numeric($error_code = $e->getCode()))
                        {
                            if ($this->_connection)
                            {
                                $error_code = $this->_connection->errorinfo();
                                $error_code = $error_code[1];
                            }
                            else
                            {
                                $error_code = 0;
                            }
                        }

                        throw new \Database_Exception($e->getMessage().' with query: "'.$sql.'"', $error_code, $e);
                    }
                }

                // no more attempts left, bail out
                else
                {
                    // and convert the exception in a database exception
                    if (!is_numeric($error_code = $e->getCode()))
                    {
                        if ($this->_connection)
                        {
                            $error_code = $this->_connection->errorinfo();
                            $error_code = $error_code[1];
                        }
                        else
                        {
                            $error_code = 0;
                        }
                    }
                    throw new \Database_Exception($e->getMessage().' with query: "'.$sql.'"', $error_code, $e);
                }
            }
        }
        while ($attempts-- > 0);

        if (isset($benchmark))
        {
            \Profiler::stop($benchmark);
        }

        // Set the last query
        $this->last_query = $sql;

        if ($type === \DB::SELECT)
        {
            // Convert the result into an array, as PDOStatement::rowCount is not reliable
            if ($as_object === false)
            {
                $result = $result->fetchAll(\PDO::FETCH_ASSOC);
            }
            elseif (is_string($as_object))
            {
                $result = $result->fetchAll(\PDO::FETCH_CLASS, $as_object);
            }
            else
            {
                $result = $result->fetchAll(\PDO::FETCH_CLASS, 'stdClass');
            }

            // Return an iterator of results
            return new \Database_Result_Cached($result, $sql, $as_object);
        }
        elseif ($type === \DB::INSERT)
        {
            // Return a list of insert id and rows created
            return array(
                $this->_connection->lastInsertId(),
                $result->rowCount(),
            );
        }
        elseif ($type === \DB::UPDATE or $type === \DB::DELETE)
        {
            // Return the number of rows affected
            return $result->errorCode() === '00000' ? $result->rowCount() : -1;
        }

        return $result->errorCode() === '00000' ? true : false;
    }

    public function list_tables($like = null)
    {
        $this->_connection or $this->connect();
        if (is_string($like))
        {
            $q = $this->_connection->prepare("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE ".$this->quote($like));
        }
        else
        {
            $q = $this->_connection->prepare("SELECT * FROM INFORMATION_SCHEMA.TABLES");
        }
        $q->execute();
        $result = $q->fetchAll();
        $tables = array();
        foreach ($result as $row)
        {
            $tables[$row['TABLE_NAME']]['NAME']   = $row['TABLE_NAME'];
            $tables[$row['TABLE_NAME']]['SCHEMA'] = $row['TABLE_SCHEMA'];
            $tables[$row['TABLE_NAME']]['TYPE']   = $row['TABLE_TYPE'];
        }
        return $tables;
    }

    public function list_columns($table, $like = null)
    {
        $this->_connection or $this->connect();
        $q       = $this->_connection->prepare("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '".$table."'");
        $q->execute();
        $result  = $q->fetchAll();
        $count   = 0;
        $columns = array();
        !is_null($like) and $like    = str_replace('%', '.*', $like);
        foreach ($result as $row)
        {
            list($type, $length) = $this->_parse_type($row['DATA_TYPE']);
            $column                     = $this->datatype($type);
            $column['name']             = $row['COLUMN_NAME'];
            $column['default']          = $row['COLUMN_DEFAULT'];
            $column['data_type']        = $type;
            $column['null']             = ($row['IS_NULLABLE'] == 'YES');
            $column['ordinal_position'] = ++$count;
            switch ($column['type'])
            {
                case 'float':
                    if (isset($length))
                    {
                        list($column['numeric_precision'], $column['numeric_scale']) = explode(',', $length);
                    }
                    break;
                case 'int':
                    if (isset($length))
                    {
                        // MySQL attribute
                        $column['display'] = $length;
                    }
                    break;
                case 'string':
                    switch ($column['data_type'])
                    {
                        case 'binary':
                        case 'varbinary':
                            $column['character_maximum_length'] = $length;
                            break;

                        case 'char':
                        case 'varchar':
                            $column['character_maximum_length'] = $length;
                        case 'text':
                        case 'tinytext':
                        case 'mediumtext':
                        case 'longtext':
                            $column['collation_name']           = isset($row['Collation']) ? $row['Collation'] : null;
                            break;

                        case 'enum':
                        case 'set':
                            $column['collation_name'] = isset($row['Collation']) ? $row['Collation'] : null;
                            $column['options']        = explode('\',\'', substr($length, 1, - 1));
                            break;
                    }
                    break;
            }
            $columns[$row['COLUMN_NAME']] = $column;
        }

        return $columns;
    }

    public function datatype($type)
    {
        // try to determine the datatype
        $datatype = parent::datatype($type);

        // if not an ANSI database, assume it's string
        return empty($datatype) ? array('type' => 'string') : $datatype;
    }

    public function escape($value)
    {
        // Make sure the database is connected
        $this->_connection or $this->connect();

        $result = $this->_connection->quote($value);

        // poor-mans workaround for the fact that not all drivers implement quote()
        if (empty($result))
        {
            if (!is_numeric($value))
            {
                $result = "'".str_replace("'", "''", $value)."'";
            }
        }
        return $result;
    }

    public function error_info()
    {
        return $this->_connection->errorInfo();
    }

    protected function _connect()
    {
        // sql server pdo connection not support
        /*
          $this->_connection = new \PDO(
          $this->_config['connection']['dsn'],
          $this->_config['connection']['username'],
          $this->_config['connection']['password'],
          $this->_config['attrs']
          );
         * 
         */
        $this->_connection = new \PDO(
                $this->_config['connection']['dsn'], $this->_config['connection']['username'], $this->_config['connection']['password']
        );
        // set the DB charset if needed
        $this->set_charset($this->_config['charset']);
        // set attribute for microsoft sql server
        $this->_connection->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
    }

    protected function driver_start_transaction()
    {
        $this->_connection or $this->connect();
        return $this->_connection->beginTransaction();
    }

    protected function driver_commit()
    {
        return $this->_connection->commit();
    }

    protected function driver_rollback()
    {
        return $this->_connection->rollBack();
    }

    protected function set_savepoint($name)
    {
        $result = $this->_connection->exec('SAVEPOINT LEVEL'.$name);
        return $result !== false;
    }

    protected function release_savepoint($name)
    {
        $result = $this->_connection->exec('RELEASE SAVEPOINT LEVEL'.$name);
        return $result !== false;
    }

    protected function rollback_savepoint($name)
    {
        $result = $this->_connection->exec('ROLLBACK TO SAVEPOINT LEVEL'.$name);
        return $result !== false;
    }

    public function select(array $args = NULL)
    {
        return new Database_Mssql_Builder_Select($args);
    }

    public function delete($table = null)
    {
        return new Database_Mssql_Builder_Delete($table);
    }

}
