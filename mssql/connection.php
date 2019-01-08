<?php

namespace Fuel\Core;

class Database_Mssql_Connection extends \Database_PDO_Connection
{

    protected $_connection;
    protected $_identifier = '';
    protected function __construct($name, array $config)
    {
        if (strpos(php_uname('s'), 'Windows') === false)
        {
            throw new \Database_Exception('The "SQLSRV" database driver works only on Windows. On *nix, use the "DBLib" driver instead.');
        }
        parent::__construct($name, $config);
        if (isset($config['identifier']))
        {
            $this->_identifier = (string) $this->_config['identifier'];
        }
    }
    public function connect()
    {
        if ($this->_connection)
        {
            return;
        }
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
        if (!empty($this->_config['connection']['persistent']))
        {
            $this->_config['attrs'][\PDO::ATTR_PERSISTENT] = true;
        }
        try
        {
            $this->_connect();
        }
        catch (\PDOException $e)
        {
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
        $this->_connection = null;
        $this->_transaction_depth = 0;
        return true;
    }
    public function driver_name()
    {
        $this->_connection or $this->connect();
        return $this->_connection->getAttribute(\PDO::ATTR_DRIVER_NAME);
    }
    public function set_charset($charset)
    {
        if ($charset == 'utf8' or $charset = 'utf-8')
        {
            $this->_connection->setAttribute(\PDO::SQLSRV_ATTR_ENCODING, \PDO::SQLSRV_ENCODING_UTF8);
        }
        elseif ($charset == 'system')
        {
            $this->_connection->setAttribute(\PDO::SQLSRV_ATTR_ENCODING, \PDO::SQLSRV_ENCODING_SYSTEM);
        }
        elseif (is_numeric($charset))
        {
            $this->_connection->setAttribute(\PDO::SQLSRV_ATTR_ENCODING, $charset);
        }
        else
        {
            $this->_connection->setAttribute(\PDO::SQLSRV_ATTR_ENCODING, \PDO::SQLSRV_ENCODING_DEFAULT);
        }
    }

    public function query($type, $sql, $as_object)
    {
        $this->_connection or $this->connect();
        if (!empty($this->_config['profiling']))
        {
            $paths = \Config::get('profiling_paths');
            $stacktrace = array();
            $include = false;
            foreach (debug_backtrace() as $index => $page)
            {
                if ($index > 0 and empty($page['file']) === false)
                {
                    foreach ($paths as $index => $path)
                    {
                        if (strpos($page['file'], $path) !== false)
                        {
                            $include = true;
                            break;
                        }
                    }
                    if ($include or empty($paths))
                    {
                        $stacktrace[] = array('file' => \Fuel::clean_path($page['file']), 'line' => $page['line']);
                    }
                }
            }
            $benchmark = \Profiler::start($this->_instance, $sql, $stacktrace);
        }
        $attempts = 3;
        do
        {
            try
            {
                $result = $this->_connection->query($sql);
                break;
            }
            catch (\Exception $e)
            {
                if ($attempts > 0)
                {
                    if (strpos($e->getMessage(), '2006 MySQL') !== false)
                    {
                        $this->disconnect();
                        $this->connect();
                    }
                    else
                    {
                        isset($benchmark) and \Profiler::delete($benchmark);
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
                else
                {
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
        $this->last_query = $sql;
        if ($type === \DB::SELECT)
        {
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
            return new \Database_Mssql_Cached($result, $sql, $as_object);
        }
        elseif ($type === \DB::INSERT)
        {
            return array(
                $this->_connection->lastInsertId(),
                $result->rowCount(),
            );
        }
        elseif ($type === \DB::UPDATE or $type === \DB::DELETE)
        {
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
        $datatype = parent::datatype($type);
        return empty($datatype) ? array('type' => 'string') : $datatype;
    }

    public function escape($value)
    {
        $this->_connection or $this->connect();
        $result = $this->_connection->quote($value);
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
        $this->_connection = new \PDO(
                $this->_config['connection']['dsn'], $this->_config['connection']['username'], $this->_config['connection']['password']
        );
        $this->set_charset($this->_config['charset']);
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
